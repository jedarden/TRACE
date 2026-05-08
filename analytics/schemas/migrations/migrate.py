#!/usr/bin/env python3
"""
Iceberg Migration Runner for TRACE

Applies schema migrations to Iceberg tables with safety checks and rollback support.

Usage:
    python migrate.py --dry-run                    # Preview what would be applied
    python migrate.py --apply V002                 # Apply specific migration
    python migrate.py --apply-all                  # Apply all pending migrations
    python migrate.py --status                     # Show migration status
    python migrate.py --rollback V002              # Rollback to previous snapshot

Requirements:
    - trino Python package
    - Access to Trino/Iceberg catalog
"""

import argparse
import sys
from pathlib import Path
from typing import List, Optional, Tuple
import re
import hashlib


class Migration:
    """Represents a single schema migration."""

    def __init__(self, path: Path):
        self.path = path
        self.filename = path.name
        self.version, self.description = self._parse_filename()
        self.content = path.read_text()
        self.checksum = self._compute_checksum()

    def _parse_filename(self) -> Tuple[int, str]:
        """Parse version and description from filename like V002__add_referrer.sql"""
        match = re.match(r'V(\d+)__(.+)\.sql', self.filename)
        if not match:
            raise ValueError(f"Invalid migration filename: {self.filename}")
        version = int(match.group(1))
        description = match.group(2).replace('_', ' ')
        return version, description

    def _compute_checksum(self) -> str:
        """Compute SHA256 checksum of migration content."""
        return hashlib.sha256(self.content.encode()).hexdigest()[:16]


class MigrationRunner:
    """Manages Iceberg schema migrations."""

    def __init__(self, migrations_dir: Path, trino_uri: str = "http://localhost:8080",
                 catalog: str = "iceberg", schema: str = "trace"):
        self.migrations_dir = migrations_dir
        self.trino_uri = trino_uri
        self.catalog = catalog
        self.schema = schema
        self.trino = None

    def _get_trino_connection(self):
        """Lazy import and return Trino connection."""
        if self.trino is None:
            try:
                import trino
                self.trino = trino.dbapi.connect(
                    host=self.trino_uri.replace('http://', '').replace('https://', '').split(':')[0],
                    port=int(self.trino_uri.split(':')[-1]) if ':' in self.trino_uri else 8080,
                    catalog=self.catalog,
                    schema=self.schema,
                    user='migration-runner'
                )
            except ImportError:
                print("Error: trino package not installed. Install with: pip install trino")
                sys.exit(1)
            except Exception as e:
                print(f"Error connecting to Trino: {e}")
                sys.exit(1)
        return self.trino

    def _execute_query(self, query: str, fetch: bool = True) -> Optional[List]:
        """Execute a SQL query and optionally return results."""
        conn = self._get_trino_connection()
        cur = conn.cursor()
        try:
            cur.execute(query)
            if fetch:
                columns = [desc[0] for desc in cur.description]
                results = [dict(zip(columns, row)) for row in cur.fetchall()]
                return results
            return None
        except Exception as e:
            print(f"Query error: {e}")
            print(f"Query was: {query[:200]}...")
            raise
        finally:
            cur.close()

    def get_applied_migrations(self) -> List[dict]:
        """Get list of already applied migrations from schema_migrations table."""
        # First check if table exists
        check_query = f"""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = '{self.schema}'
            AND table_name = 'schema_migrations'
        """

        result = self._execute_query(check_query)
        if not result:
            print("Schema migrations table not found. Run V001__initial_schema.sql first.")
            return []

        query = f"""
        SELECT version, description, applied_at, applied_by, checksum
        FROM {self.schema}.schema_migrations
        ORDER BY version ASC
        """
        return self._execute_query(query) or []

    def get_pending_migrations(self) -> List[Migration]:
        """Get list of migrations that haven't been applied yet."""
        applied = {m['version'] for m in self.get_applied_migrations()}

        migration_files = sorted(
            self.migrations_dir.glob('V*.sql'),
            key=lambda p: int(re.match(r'V(\d+)', p.name).group(1))
        )

        pending = []
        for path in migration_files:
            migration = Migration(path)
            if migration.version not in applied:
                pending.append(migration)

        return pending

    def get_snapshot_before_migration(self, version: int) -> Optional[str]:
        """Get the snapshot ID from before a migration was applied."""
        query = f"""
        SELECT snapshot_id, committed_at
        FROM {self.schema}.ad_events.snapshots
        WHERE summary['migration'] = 'V{version}'
        ORDER BY committed_at ASC
        LIMIT 1
        """
        results = self._execute_query(query)
        if results and len(results) > 1:
            return results[0]['snapshot_id']
        return None

    def show_status(self):
        """Display current migration status."""
        applied = self.get_applied_migrations()
        pending = self.get_pending_migrations()

        print(f"\n{'='*60}")
        print(f"Migration Status for {self.catalog}.{self.schema}")
        print(f"{'='*60}\n")

        if applied:
            print(f"Applied Migrations ({len(applied)}):")
            for m in applied:
                print(f"  V{m['version']:03d} - {m['description']} ({m['applied_at']})")
        else:
            print("No migrations applied yet.")

        if pending:
            print(f"\nPending Migrations ({len(pending)}):")
            for m in pending:
                print(f"  V{m.version:03d} - {m.description}")
        else:
            print("\n✓ All migrations applied!")

        print(f"\n{'='*60}\n")

    def apply_migration(self, migration: Migration, dry_run: bool = False) -> bool:
        """Apply a single migration."""
        print(f"\nApplying V{migration.version:03d}: {migration.description}")
        print(f"  File: {migration.path}")
        print(f"  Checksum: {migration.checksum}")

        if dry_run:
            print("  [DRY RUN] Would apply this migration")
            return True

        # Read and execute migration SQL
        try:
            # For complex migrations, we might need to execute statements separately
            # For now, we'll execute the whole file
            conn = self._get_trino_connection()
            cur = conn.cursor()

            # Add migration marker to snapshot summary
            migration_marker = f"\nALTER TABLE {self.schema}.ad_events SET TPROPERTIES ('migration' = 'V{migration.version}');"

            full_sql = migration.content + migration_marker

            cur.execute(full_sql)
            conn.commit()
            cur.close()

            print(f"  ✓ Migration V{migration.version:03d} applied successfully")
            return True

        except Exception as e:
            print(f"  ✗ Migration failed: {e}")
            print(f"  Consider using time travel to rollback:")
            print(f"    SELECT * FROM {self.schema}.ad_events FOR VERSION AS OF <snapshot_id>;")
            return False

    def apply_all_pending(self, dry_run: bool = False) -> bool:
        """Apply all pending migrations in order."""
        pending = self.get_pending_migrations()

        if not pending:
            print("No pending migrations to apply.")
            return True

        print(f"Applying {len(pending)} pending migrations...")

        for migration in pending:
            if not self.apply_migration(migration, dry_run):
                return False

        return True


def main():
    parser = argparse.ArgumentParser(
        description="Iceberg Migration Runner for TRACE",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --status                   Show migration status
  %(prog)s --dry-run                  Preview pending migrations
  %(prog)s --apply-all                Apply all pending migrations
  %(prog)s --apply V002               Apply specific migration
  %(prog)s --rollback V002            Show rollback info for migration
        """
    )

    parser.add_argument('--migrations-dir', type=Path, default='analytics/schemas/migrations',
                        help='Directory containing migration SQL files')
    parser.add_argument('--trino-uri', default='http://localhost:8080',
                        help='Trino server URI')
    parser.add_argument('--catalog', default='iceberg',
                        help='Iceberg catalog name')
    parser.add_argument('--schema', default='trace',
                        help='Schema name')

    action = parser.add_mutually_exclusive_group(required=True)
    action.add_argument('--status', action='store_true',
                        help='Show migration status')
    action.add_argument('--dry-run', action='store_true',
                        help='Preview pending migrations without applying')
    action.add_argument('--apply-all', action='store_true',
                        help='Apply all pending migrations')
    action.add_argument('--apply', metavar='VERSION',
                        help='Apply specific migration (e.g., V002)')
    action.add_argument('--rollback', metavar='VERSION',
                        help='Show rollback info for migration')

    args = parser.parse_args()

    runner = MigrationRunner(
        migrations_dir=args.migrations_dir,
        trino_uri=args.trino_uri,
        catalog=args.catalog,
        schema=args.schema
    )

    if args.status:
        runner.show_status()

    elif args.dry_run:
        pending = runner.get_pending_migrations()
        print(f"\nPending migrations ({len(pending)}):")
        for m in pending:
            print(f"  V{m.version:03d} - {m.description}")

        if pending:
            print(f"\nWould apply {len(pending)} migrations.")
            print("Run with --apply-all to apply them.")

    elif args.apply_all:
        if runner.apply_all_pending(dry_run=False):
            print("\n✓ All migrations applied successfully!")
            runner.show_status()
        else:
            print("\n✗ Migration failed. Check the error above.")
            sys.exit(1)

    elif args.apply:
        # Parse version (V002 -> 2)
        version = int(args.apply.upper().replace('V', ''))
        pending = runner.get_pending_migrations()

        migration = next((m for m in pending if m.version == version), None)
        if not migration:
            print(f"Migration V{version:03d} not found or already applied.")
            sys.exit(1)

        if runner.apply_migration(migration, dry_run=False):
            print("\n✓ Migration applied successfully!")
        else:
            print("\n✗ Migration failed.")
            sys.exit(1)

    elif args.rollback:
        version = int(args.rollback.upper().replace('V', ''))
        snapshot = runner.get_snapshot_before_migration(version)

        if snapshot:
            print(f"\nRollback information for V{version:03d}:")
            print(f"  Snapshot before migration: {snapshot}")
            print(f"\nTo rollback, query using time travel:")
            print(f"  SELECT * FROM {args.schema}.ad_events FOR VERSION AS OF {snapshot};")
            print(f"\nOr create a rollback view:")
            print(f"  CREATE VIEW {args.schema}.ad_events_pre_v{version:03d} AS")
            print(f"  SELECT * FROM {args.schema}.ad_events FOR VERSION AS OF {snapshot};")
        else:
            print(f"Could not find snapshot information for V{version:03d}")
            print("Available snapshots:")
            query = f"SELECT snapshot_id, committed_at, summary FROM {args.schema}.ad_events.snapshots ORDER BY committed_at DESC LIMIT 10;"
            print(query)


if __name__ == '__main__':
    main()
