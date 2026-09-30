//! Fixture tests for the cross-network normalization views
//! (`docs/analytics/normalization.sql`).
//!
//! The view definitions are loaded straight from the documentation file with
//! the S3 `read_parquet` source swapped for an in-memory fixtures table, so
//! the SQL under test is the SQL analysts run — not a hand-copied variant.
//!
//! Expected values mirror the flusher's Rust normalizer
//! (`flusher/src/normalizer.rs` + `flusher/src/network_mapping.toml`): same
//! canonical fields, same first-match-wins parameter lists, same detection
//! order (utm_source alias -> click identifier -> signature parameter).
//! The googleads fixtures are the SQL counterparts of that module's
//! `test_normalize_googleads`; the other networks pin the pre-existing
//! behavior so adding googleads cannot quietly regress it.

use duckdb::Connection;
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Path to the doc file, relative to this crate (`analytics/`).
const NORMALIZATION_SQL: &str = "../docs/analytics/normalization.sql";

/// The live view's source in `normalization.sql`. It must be the FIRST
/// occurrence in the file — later ones sit inside comment blocks.
const SOURCE: &str = "read_parquet(\n        's3://my-trace-bucket/trace-events/iceberg/ad_events/data/**/*.parquet',\n        hive_partitioning = true\n    )";

static TEMP_DIR_SEQUENCE: AtomicUsize = AtomicUsize::new(0);

/// One fixture event: the label doubles as the event `url`, so assertions can
/// select their row by `url`.
struct Fixture {
    label: &'static str,
    params: &'static [(&'static str, &'static str)],
}

const FIXTURES: &[Fixture] = &[
    // Mirrors the Rust normalizer's test_normalize_googleads: detected via
    // the gclid click identifier, campaignid stands in for utm_campaign, the
    // ad group fills ad_id/creative_id/item_id, and the matched keyword
    // becomes the headline.
    Fixture {
        label: "googleads-gclid",
        params: &[
            ("gclid", "test123"),
            ("campaignid", "camp456"),
            ("adgroupid", "adgroup789"),
            ("keyword", "best running shoes"),
        ],
    },
    // utm_source=google alias, with the fields the doc's supported-network
    // table names: imageid -> image_id, placement -> headline, targetid ->
    // placement_id, siteid -> publisher_id.
    Fixture {
        label: "googleads-utm-source",
        params: &[
            ("utm_source", "google"),
            ("utm_campaign", "camp-g"),
            ("imageid", "img-987"),
            ("placement", "placement-42"),
            ("siteid", "site-1"),
            ("targetid", "tgt-7"),
        ],
    },
    // Shopping ads: feeditemid takes ad_id/creative_id/item_id, and gclsrc
    // (not gclid) drives detection. utm_content must NOT override feeditemid
    // for creative_id — first match wins in the Rust mapping list.
    Fixture {
        label: "googleads-feeditem",
        params: &[
            ("gclsrc", "aw.l"),
            ("utm_campaign", "camp-feed"),
            ("feeditemid", "feed-1"),
            ("utm_content", "creative-x"),
        ],
    },
    // utm_source value case-insensitivity, matching the Rust normalizer's
    // lowercased comparison.
    Fixture {
        label: "googleads-utm-source-case",
        params: &[("utm_source", "GOOGLE"), ("utm_campaign", "camp-upper")],
    },
    // Mirrors test_normalize_taboola.
    Fixture {
        label: "taboola-full",
        params: &[
            ("utm_source", "taboola"),
            ("utm_campaign", "camp123"),
            ("tb_image", "img-abc"),
            ("tb_headline", "Click Here Now"),
            ("tb_item", "item-456"),
            ("tb_publisher", "pub-789"),
            ("tb_placement", "placement-123"),
        ],
    },
    // utm_source alias tb -> taboola.
    Fixture {
        label: "taboola-alias",
        params: &[("utm_source", "tb"), ("tb_image", "alias-img")],
    },
    // Mirrors test_normalize_mgid.
    Fixture {
        label: "mgid-full",
        params: &[
            ("utm_source", "mgid"),
            ("mg_id", "mg-789"),
            ("mg_title", "Doctors Hate Him"),
            ("mg_image", "mg-img-123"),
            ("mg_site", "example.com"),
        ],
    },
    // Detected from signature parameters alone (no utm_source).
    Fixture {
        label: "outbrain-params",
        params: &[("ob_creative", "ob-1"), ("ob_item", "ob-item-1")],
    },
    Fixture {
        label: "revcontent-params",
        params: &[
            ("rc_id", "rc123"),
            ("rc_title", "rc title"),
            ("rc_thumb", "thumb-9"),
        ],
    },
    // Mirrors test_normalize_unknown: generic fallback mapping.
    Fixture {
        label: "unknown-generic",
        params: &[("utm_campaign", "unknown_camp"), ("item", "item123")],
    },
];

/// `network`, then the eight canonical fields: campaign_id, ad_id,
/// creative_id, publisher_id, placement_id, headline, image_id, item_id.
type Row = (
    String,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
);

/// Open an in-memory DuckDB, install the fixtures, and build the
/// normalization views over them.
fn open_db() -> Connection {
    let conn = Connection::open_in_memory().unwrap();

    conn.execute_batch(
        "CREATE TABLE fixtures (
            ts TIMESTAMP,
            ts_day DATE,
            ip VARCHAR,
            ua VARCHAR,
            url VARCHAR,
            type VARCHAR,
            params MAP(VARCHAR, VARCHAR)
        )",
    )
    .unwrap();

    for f in FIXTURES {
        let entries: Vec<String> = f
            .params
            .iter()
            .map(|(k, v)| format!("['{}', '{}']", k.replace('\'', "''"), v.replace('\'', "''")))
            .collect();
        let (keys, values): (Vec<String>, Vec<String>) = entries
            .iter()
            .map(|e| (e[..e.len() / 2].to_string(), e[e.len() / 2..].to_string()))
            .unzip();
        let _ = (keys, values);

        // Fixture values are compile-time constants with no quoting hazards;
        // the replace() above is belt and braces for future edits.
        let map = format!(
            "MAP([{}], [{}])",
            f.params
                .iter()
                .map(|(k, _)| format!("'{}'", k.replace('\'', "''")))
                .collect::<Vec<_>>()
                .join(", "),
            f.params
                .iter()
                .map(|(_, v)| format!("'{}'", v.replace('\'', "''")))
                .collect::<Vec<_>>()
                .join(", "),
        );
        conn.execute(
            &format!(
                "INSERT INTO fixtures VALUES (TIMESTAMP '2026-09-01 12:00:00', DATE '2026-09-01', \
                 '192.0.2.1', 'test-ua', '{}', 'click', {map})",
                f.label.replace('\'', "''"),
            ),
            [],
        )
        .unwrap_or_else(|e| panic!("inserting fixture {}: {e}", f.label));
    }

    initialize_views(
        &conn,
        "(SELECT ts, ts_day, ip, ua, url, type, params FROM fixtures)",
    );

    conn
}

/// Load the documented views from the SQL file, swapping only the Parquet
/// source so these checks never need network access or an installed extension.
fn initialize_views(conn: &Connection, source: &str) {
    let sql = fs::read_to_string(manifest_dir().join(NORMALIZATION_SQL))
        .expect("reading normalization.sql from docs/");
    assert!(
        sql.contains(SOURCE),
        "normalization.sql no longer contains the expected S3 source; \
         update SOURCE in this test"
    );
    assert!(
        sql.contains("INSTALL httpfs;"),
        "missing httpfs installation"
    );
    assert!(sql.contains("LOAD httpfs;"), "missing httpfs load");
    assert!(
        sql.contains("SET s3_region = 'us-east-1';"),
        "missing S3 region"
    );
    // Swap only the live view's source (first occurrence); the remaining
    // occurrences live inside comment blocks, which get stripped below.
    let sql = sql.replacen(SOURCE, source, 1);
    let sql = strip_block_comments(&sql);
    for (index, statement) in sql.split(';').enumerate() {
        if statement.trim().is_empty() {
            continue;
        }
        // Extensions and S3 configuration are exercised by the documented
        // CLI workflow; fixture tests intentionally stay local and offline.
        if ["INSTALL HTTPFS", "LOAD HTTPFS", "SET S3_REGION"]
            .iter()
            .any(|prefix| statement.trim().to_uppercase().starts_with(prefix))
        {
            continue;
        }
        conn.execute_batch(statement)
            .unwrap_or_else(|error| panic!("building normalization view {}: {error}", index + 1));
    }
}

fn manifest_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn strip_block_comments(sql: &str) -> String {
    let mut out = String::with_capacity(sql.len());
    let mut in_block = false;
    for line in sql.lines() {
        let trimmed = line.trim();
        if !in_block && trimmed.starts_with("/*") {
            in_block = true;
            continue;
        }
        if in_block {
            if trimmed.ends_with("*/") {
                in_block = false;
            }
            continue;
        }
        let sql_line = line.split_once("--").map_or(line, |(sql, _)| sql);
        out.push_str(sql_line);
        out.push('\n');
    }
    out
}

fn normalized_row(conn: &Connection, fixture: &str) -> Row {
    conn.query_row(
        "SELECT network, campaign_id, ad_id, creative_id, publisher_id, \
                placement_id, headline, image_id, item_id
         FROM normalized_campaigns
         WHERE url = ?",
        [fixture],
        |r| {
            Ok((
                r.get(0)?,
                r.get(1)?,
                r.get(2)?,
                r.get(3)?,
                r.get(4)?,
                r.get(5)?,
                r.get(6)?,
                r.get(7)?,
                r.get(8)?,
            ))
        },
    )
    .unwrap_or_else(|e| panic!("querying fixture {fixture}: {e}"))
}

#[test]
fn normalized_campaigns_exposes_the_hive_day_for_pruned_queries() {
    let conn = open_db();
    let day: String = conn
        .query_row(
            "SELECT CAST(ts_day AS VARCHAR) FROM normalized_campaigns WHERE url = 'googleads-gclid'",
            [],
            |row| row.get(0),
        )
        .expect("normalized view should preserve the Hive partition column");
    assert_eq!(day, "2026-09-01");
}

fn s(v: &Option<String>) -> Option<&str> {
    v.as_deref()
}

macro_rules! assert_fixture {
    ($conn:expr, $label:expr, $network:expr,
     [ $campaign:expr, $ad:expr, $creative:expr, $publisher:expr,
       $placement:expr, $headline:expr, $image:expr, $item:expr $(,)? ]) => {{
        let row = normalized_row($conn, $label);
        assert_eq!(row.0, $network, "network for {}", $label);
        let got: [Option<&str>; 8] = [
            s(&row.1),
            s(&row.2),
            s(&row.3),
            s(&row.4),
            s(&row.5),
            s(&row.6),
            s(&row.7),
            s(&row.8),
        ];
        let expected: [Option<&str>; 8] = [
            $campaign, $ad, $creative, $publisher, $placement, $headline, $image, $item,
        ];
        const NAMES: [&str; 8] = [
            "campaign_id",
            "ad_id",
            "creative_id",
            "publisher_id",
            "placement_id",
            "headline",
            "image_id",
            "item_id",
        ];
        for i in 0..8 {
            assert_eq!(
                expected[i], got[i],
                "{} of fixture {} (expected {:?}, got {:?})",
                NAMES[i], $label, expected[i], got[i]
            );
        }
    }};
}

#[test]
fn normalization_views_match_rust_normalizer_fixtures() {
    let conn = open_db();

    // googleads via gclid click identifier — the Rust normalizer's
    // test_normalize_googleads, expressed as SQL.
    assert_fixture!(
        &conn,
        "googleads-gclid",
        "googleads",
        [
            Some("camp456"),
            Some("adgroup789"),
            Some("adgroup789"),
            None,
            None,
            Some("best running shoes"),
            None,
            Some("adgroup789"),
        ]
    );

    // googleads via utm_source alias, with imageid/placement/siteid/targetid.
    assert_fixture!(
        &conn,
        "googleads-utm-source",
        "googleads",
        [
            Some("camp-g"),
            None,
            None,
            Some("site-1"),
            Some("tgt-7"),
            Some("placement-42"),
            Some("img-987"),
            None,
        ]
    );

    // Shopping ads: feeditemid wins creative_id before utm_content; gclsrc
    // drives detection.
    assert_fixture!(
        &conn,
        "googleads-feeditem",
        "googleads",
        [
            Some("camp-feed"),
            Some("feed-1"),
            Some("feed-1"),
            None,
            None,
            None,
            None,
            Some("feed-1"),
        ]
    );

    // utm_source value case is normalized.
    assert_fixture!(
        &conn,
        "googleads-utm-source-case",
        "googleads",
        [Some("camp-upper"), None, None, None, None, None, None, None,]
    );

    // Pre-existing networks: pin their mappings so the googleads work cannot
    // regress them.
    assert_fixture!(
        &conn,
        "taboola-full",
        "taboola",
        [
            Some("camp123"),
            Some("item-456"),
            Some("img-abc"),
            Some("pub-789"),
            Some("placement-123"),
            Some("Click Here Now"),
            Some("img-abc"),
            Some("item-456"),
        ]
    );

    assert_fixture!(
        &conn,
        "taboola-alias",
        "taboola",
        [
            None,
            None,
            Some("alias-img"),
            None,
            None,
            None,
            Some("alias-img"),
            None,
        ]
    );

    assert_fixture!(
        &conn,
        "mgid-full",
        "mgid",
        [
            None,
            Some("mg-789"),
            Some("mg-789"),
            Some("example.com"),
            None,
            Some("Doctors Hate Him"),
            Some("mg-img-123"),
            Some("mg-789"),
        ]
    );

    assert_fixture!(
        &conn,
        "outbrain-params",
        "outbrain",
        [
            None,
            Some("ob-item-1"),
            Some("ob-1"),
            None,
            None,
            None,
            Some("ob-1"),
            Some("ob-item-1"),
        ]
    );

    assert_fixture!(
        &conn,
        "revcontent-params",
        "revcontent",
        [
            None,
            Some("rc123"),
            Some("rc123"),
            None,
            None,
            Some("rc title"),
            Some("thumb-9"),
            Some("rc123"),
        ]
    );

    // Generic fallback for unknown traffic.
    assert_fixture!(
        &conn,
        "unknown-generic",
        "unknown",
        [
            Some("unknown_camp"),
            Some("item123"),
            None,
            None,
            None,
            None,
            None,
            Some("item123"),
        ]
    );
}

#[test]
fn every_fixture_is_detected_exactly_once() {
    let conn = open_db();

    let mut stmt = conn
        .prepare(
            "SELECT network, COUNT(*) FROM normalized_campaigns GROUP BY network ORDER BY network",
        )
        .unwrap();
    let rows = stmt
        .query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?)))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    assert_eq!(
        rows,
        vec![
            ("googleads".to_string(), 4),
            ("mgid".to_string(), 1),
            ("outbrain".to_string(), 1),
            ("revcontent".to_string(), 1),
            ("taboola".to_string(), 2),
            ("unknown".to_string(), 1),
        ],
        "every fixture must land in exactly one network bucket"
    );
}

/// Populate enough recent and prior traffic to make all five documented
/// views return representative results. The lower recent CTR also exercises
/// the fatigue calculation rather than only its empty-result path.
fn insert_performance_fixtures(conn: &Connection) {
    conn.execute_batch(
        "INSERT INTO fixtures
         WITH periods(event_day, daily_clicks) AS (
             VALUES (CAST(CURRENT_DATE + INTERVAL '-1 day' AS DATE), 12),
                    (CAST(CURRENT_DATE + INTERVAL '-2 days' AS DATE), 12),
                    (CAST(CURRENT_DATE + INTERVAL '-3 days' AS DATE), 12),
                    (CAST(CURRENT_DATE + INTERVAL '-10 days' AS DATE), 20),
                    (CAST(CURRENT_DATE + INTERVAL '-11 days' AS DATE), 20),
                    (CAST(CURRENT_DATE + INTERVAL '-12 days' AS DATE), 20)
         ),
         networks(network, campaign, creative) AS (
             VALUES ('taboola', 'camp-taboola', 'creative-taboola'),
                    ('outbrain', 'camp-outbrain', 'creative-outbrain')
         ),
         events AS (
             SELECT d.event_day, n.network, n.campaign, n.creative,
                    'pageview' AS event_type, p.event_index
             FROM periods d CROSS JOIN networks n CROSS JOIN range(100) p(event_index)
             UNION ALL
             SELECT d.event_day, n.network, n.campaign, n.creative,
                    'click' AS event_type, p.event_index
             FROM periods d CROSS JOIN networks n
             CROSS JOIN LATERAL range(d.daily_clicks) p(event_index)
         )
         SELECT
             CAST(event_day AS TIMESTAMP) + INTERVAL '12 hours',
             event_day,
             '192.0.2.2',
             'analytics-view-test',
             'analytics-' || network || '-' || CAST(event_day AS VARCHAR) || '-' || event_type || '-' || event_index,
             event_type,
             CASE network
                 WHEN 'taboola' THEN MAP(
                     ['utm_source', 'utm_campaign', 'tb_item', 'tb_image', 'tb_headline'],
                     [network, campaign, 'item-taboola', creative, 'Shared Campaign'])
                 ELSE MAP(
                     ['utm_source', 'utm_campaign', 'ob_item', 'ob_creative', 'headline'],
                     [network, campaign, 'item-outbrain', creative, 'Shared Campaign'])
             END
         FROM events",
    )
    .expect("insert performance view fixtures");
}

fn assert_performance_views(conn: &Connection) {
    let normalized: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM normalized_campaigns
             WHERE network IN ('taboola', 'outbrain') AND headline = 'Shared Campaign'",
            [],
            |row| row.get(0),
        )
        .expect("query normalized_campaigns");
    let network_counts: Vec<(String, i64)> = conn
        .prepare(
            "SELECT network, COUNT(*) FROM normalized_campaigns
             WHERE headline = 'Shared Campaign' GROUP BY network ORDER BY network",
        )
        .expect("prepare network-count diagnostic")
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
        .expect("query network-count diagnostic")
        .collect::<Result<Vec<_>, _>>()
        .expect("collect network-count diagnostic");
    assert_eq!(
        normalized,
        2 * (3 * 112 + 3 * 120),
        "both mapped networks should retain their shared headline: {network_counts:?}"
    );

    let performance_groups: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM network_performance
             WHERE network IN ('taboola', 'outbrain')",
            [],
            |row| row.get(0),
        )
        .expect("query network_performance");
    assert!(
        performance_groups >= 12,
        "both networks should have at least six days each"
    );

    let top_creatives: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM top_creatives WHERE headline = 'Shared Campaign'",
            [],
            |row| row.get(0),
        )
        .expect("query top_creatives");
    assert_eq!(top_creatives, 2, "each network's creative should qualify");

    let fatigue: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM creative_fatigue
             WHERE headline = 'Shared Campaign' AND fatigue_change_pct < 0",
            [],
            |row| row.get(0),
        )
        .expect("query creative_fatigue");
    assert_eq!(fatigue, 2, "both creatives should show declining CTR");

    let (fingerprint, network_count, clicks): (String, i64, i64) = conn
        .query_row(
            "SELECT normalized_headline, num_networks, total_clicks
             FROM cross_network_creatives LIMIT 1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .expect("query cross_network_creatives");
    assert_eq!(fingerprint, "shared campaign");
    assert_eq!(network_count, 2);
    assert_eq!(clicks, 2 * (3 * 12 + 3 * 20));
}

#[test]
fn every_documented_view_returns_representative_fixture_data() {
    let conn = open_db();
    insert_performance_fixtures(&conn);
    assert_performance_views(&conn);
}

#[test]
fn every_documented_view_reads_the_hive_partitioned_parquet_layout() {
    let root = temp_dir("normalization-partitions");
    let parquet_root = root.join("iceberg/ad_events/data");
    fs::create_dir_all(&parquet_root).expect("create Parquet output directory");

    let writer = Connection::open_in_memory().expect("open Parquet writer");
    writer
        .execute_batch(
            "CREATE TABLE fixtures (
                ts TIMESTAMP, ts_day DATE, ip VARCHAR, ua VARCHAR,
                url VARCHAR, type VARCHAR, params MAP(VARCHAR, VARCHAR)
             )",
        )
        .expect("create Parquet fixture table");
    insert_performance_fixtures(&writer);
    writer
        .execute_batch(&format!(
            "COPY fixtures TO '{}' (FORMAT PARQUET, PARTITION_BY (ts_day))",
            parquet_root.display()
        ))
        .expect("write Hive-partitioned fixture files");

    let conn = Connection::open_in_memory().expect("open partition query connection");
    let glob = format!(
        "read_parquet('{}', hive_partitioning = true)",
        parquet_root.join("**/*.parquet").display()
    );
    initialize_views(&conn, &glob);
    assert_performance_views(&conn);

    let partition_days: i64 = conn
        .query_row(
            "SELECT COUNT(DISTINCT ts_day) FROM normalized_campaigns
             WHERE headline = 'Shared Campaign'",
            [],
            |row| row.get(0),
        )
        .expect("read Hive partition column in normalized_campaigns");
    assert_eq!(partition_days, 6);

    fs::remove_dir_all(root).expect("remove Parquet fixtures");
}

fn temp_dir(label: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!(
        "trace-{}-{}-{}",
        label,
        std::process::id(),
        TEMP_DIR_SEQUENCE.fetch_add(1, Ordering::SeqCst)
    ));
    fs::create_dir_all(&path).expect("create temporary fixture directory");
    path
}
