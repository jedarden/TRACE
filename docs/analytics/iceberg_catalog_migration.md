# Iceberg catalog bootstrap

TRACE writes new batches as Hive-partitioned Parquet. The publisher can append
those objects to the `trace.ad_events` Iceberg table, but historical files
must be bootstrapped before publication is enabled.

## Catalog and storage

Deploy `k8s/iceberg-rest.yaml` through the cluster's GitOps repository. The
catalog uses a persistent SQLite JDBC catalog and the existing
`trace-s3-credentials` secret. Configure `trace-config` with:

```yaml
iceberg-catalog-uri: http://iceberg-rest.trace.svc.cluster.local:8181
iceberg-warehouse: s3://<bucket>/trace-events/iceberg
```

The REST service remains cluster-internal. Keep the catalog and the TRACE
writers pointed at the same bucket, prefix, and warehouse.

## Bootstrap existing files

Run a hierarchy sync to create the current campaign files. Pause the flusher,
compactor, and hierarchy syncer. Then run the analytics image once with its S3
credentials and `ICEBERG_CATALOG_URI` /
`ICEBERG_WAREHOUSE` configured:

```sh
trace-analytics migrate-iceberg
```

The command reads the existing `events/`, `events-compacted/`,
`iceberg/ad_events/data/`, `iceberg/sessions/data/`,
`iceberg/campaigns/data/`, and `assets/` Parquet
views. Event generations are normalized through the compatibility views. It
creates catalog tables with DuckDB CTAS, leaves source objects in place, and
verifies row counts through the catalog, `iceberg_scan`, and the initial
snapshot. If a table already exists, it proceeds only when its row count
matches the Parquet source, which makes a completed table safe to verify after
a retry without appending duplicate data. The campaigns CTAS uses Iceberg
identity partitioning by `network`.

Iceberg analytics expects `trace.campaigns` to exist in the catalog.
The syncer maintains one current Parquet file per network. CTAS creates an
initial `trace.campaigns` Iceberg snapshot from those files. After bootstrap,
enable `TRACE_ICEBERG_PUBLISH=true` for the syncer to append immutable campaign
revisions from later hierarchy syncs. The analytics `trace.campaigns` view
selects the latest revision for each `(network, campaign_id)`; its Parquet mode
reads the current hierarchy files directly. The Iceberg table retains older
revisions for time travel.

The bootstrap command is one-time; after revision publication begins, its
source-count retry check no longer applies to the campaigns table.
An existing campaigns table created from the older optional DDL has different
columns; migrate its schema before enabling revision publication.

Deployment manifests leave general flusher and compactor publication disabled;
enable the syncer's campaign publisher separately after bootstrap. The current
Iceberg compactor only implements append, so it stays paused until it gains snapshot
rewrite semantics. Enabling the flusher now would let the legacy compactor
delete files still referenced by the catalog. Do not delete or compact
catalog-referenced files by hand: older snapshots depend on them.

## Read and time-travel checks

With the analytics environment configured, run:

```sql
SELECT count(*) FROM iceberg_scan(
  's3://<bucket>/trace-events/iceberg/trace/ad_events',
  catalog_uri => 'http://iceberg-rest.trace.svc.cluster.local:8181'
);

SELECT * FROM iceberg_snapshots(trace_iceberg.trace.ad_events);

SELECT count(*) FROM iceberg_scan(
  's3://<bucket>/trace-events/iceberg/trace/ad_events',
  catalog_uri => 'http://iceberg-rest.trace.svc.cluster.local:8181',
  snapshot_from_id => <snapshot_id>
);
```

DuckDB's `iceberg_scan` path interface supports reads and time travel; catalog
writes require attaching the REST catalog. See the
[DuckDB Iceberg writing guide](https://duckdb.org/docs/current/core_extensions/iceberg/writing_to_iceberg).
