# Iceberg catalog bootstrap

TRACE writes new batches as Hive-partitioned Parquet. The publisher can append
those objects to the `trace.ad_events` Iceberg table, but historical files
must be bootstrapped before publication is enabled.

## Catalog and storage

Deploy `declarative-config/k8s/ardenone-manager/trace/iceberg-rest.yaml`
through the cluster's GitOps repository. The catalog uses a persistent SQLite
JDBC catalog and the `trace-s3-credentials` secret. Resolve the runtime
settings by reference from namespace `trace`:

| Runtime setting | In-cluster retrieval path | Off-cluster retrieval path |
| --- | --- | --- |
| `ICEBERG_CATALOG_URI` | ConfigMap `trace-config`, key `iceberg-catalog-uri` | ConfigMap `trace-config`, key `iceberg-catalog-uri` |
| `ICEBERG_WAREHOUSE` | ConfigMap `trace-config`, key `iceberg-warehouse` | ConfigMap `trace-config`, key `iceberg-warehouse` |
| `TRACE_S3_BUCKET` | Secret `trace-s3-credentials`, key `bucket` | OpenBao `secret/ardenone-manager/trace/s3-credentials`, field `bucket` |
| `TRACE_S3_REGION` | Secret `trace-s3-credentials`, key `region` | OpenBao `secret/ardenone-manager/trace/s3-credentials`, field `region` |
| `AWS_ACCESS_KEY_ID` | Secret `trace-s3-credentials`, key `access-key-id` | OpenBao `secret/ardenone-manager/trace/s3-credentials`, field `access-key-id` |
| `AWS_SECRET_ACCESS_KEY` | Secret `trace-s3-credentials`, key `secret-access-key` | OpenBao `secret/ardenone-manager/trace/s3-credentials`, field `secret-access-key` |
| `S3_ENDPOINT` | ConfigMap `trace-config`, key `s3-endpoint`, when using a non-AWS endpoint | OpenBao `secret/ardenone-manager/trace/s3-credentials`, field `endpoint`, when using a non-AWS endpoint |

The optional `s3-endpoint` ConfigMap key is not present currently; without an
endpoint setting, DuckDB uses its AWS default.

For an off-cluster run, keep S3 credentials in OpenBao under
`secret/ardenone-manager/trace/s3-credentials` and load them directly into the
executor's environment at launch. Provision values through
`bao-as ardenone-manager-provision` using the current KV version with `-cas`;
send provider output through a pipe to `bao kv put` and never place a value in
an argument, shell history, log, or file. Do not copy credential values into
this document. The in-cluster path is the Secret and key references above.

The REST service is cluster-internal. Keep the catalog and TRACE writers
pointed at the same bucket, prefix, and warehouse. The live GitOps ConfigMap
currently has a pending placeholder warehouse bucket, and the
`trace-s3-credentials` Secret and OpenBao path have not been provisioned. The
real bucket is still an operator input. Resolve the bucket, optional S3
endpoint, and credentials before running the migration. Do not create tables
against the placeholder warehouse.

## Bootstrap existing files

Run a hierarchy sync to create the current campaign files. Pause the flusher,
compactor, and hierarchy syncer. Build the local executor with
`cargo build --manifest-path analytics/Cargo.toml`, or use a published,
version-pinned analytics image. Run it in-cluster so the internal catalog
endpoint resolves, with its S3 credentials and `ICEBERG_CATALOG_URI` /
`ICEBERG_WAREHOUSE` populated from the references above:

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
