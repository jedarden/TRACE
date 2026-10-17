# Analytics fixtures

`assets/` contains the fixed local Parquet data used by
`test_asset_partition_predicates_prune_files_scanned`. It has one row and one
Parquet file for each of these Hive partitions:

| Network | Type | Asset ID |
| --- | --- | --- |
| `taboola` | `headline` | `taboola:headline` |
| `taboola` | `image` | `taboola:image` |
| `mgid` | `headline` | `mgid:headline` |
| `mgid` | `image` | `mgid:image` |

Each row also stores its network, type, and the fixed content value
`fixture content`. The test registers these files through the local Hive
Parquet view; it does not use the Iceberg catalog or remote storage.

`iceberg/campaigns/data/` contains three network-partitioned Parquet files
for `test_campaign_partition_prunes_files_and_preserves_network_key`. Two
networks deliberately share `campaign_id = shared-1` with different names
and account IDs, so the fixture checks that campaign joins use both network
and ID. The Taboola file also includes a paused campaign with no traffic.
