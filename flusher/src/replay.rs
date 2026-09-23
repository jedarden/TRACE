//! Raw-log replay and backfill.
//!
//! Raw logs are the source of truth ("log first, parse later" — if ETL logic
//! improves, replay from the beginning), but the live flusher is strictly
//! one-pass: it watches the collector's log directory, uploads each raw file
//! once, and deletes it. This module is the other half of the promise — a
//! reprocessing pass over archived `raw-YYYYMMDD-HH.jsonl[.gz|.ready]` files:
//!
//! 1. **Range selection** — pick the raw files whose hour falls inside an
//!    inclusive `--from`/`--to` hour range (`YYYYMMDD-HH`, or `YYYYMMDD` for a
//!    whole day).
//! 2. **Parsing** — the same [`RawLogParser`] the live path uses, so a replay
//!    is bit-for-bit what a fresh flush would produce from the same lines.
//! 3. **Impression dedup** — repeated sends carrying the same `imp_id` within
//!    one raw file collapse to the first arrival, matching the live path.
//! 4. **Normalization + asset extraction** — the config-driven
//!    [`NormalizationMapping`] detects the ad network from the params and maps
//!    network-specific names (`tb_headline`, `mg_title`, `ad_name`, …) onto
//!    the canonical typed columns (`network`, `campaign_id`, `creative_id`,
//!    `headline`, `image_id`, `item_id`) the live path leaves NULL until
//!    enrichment.
//! 5. **Sessionization** — the same gap-based rules as the authoritative
//!    DuckDB materializer in `analytics/src/session_materializer.rs`
//!    (30-minute inactivity split, UTC-day cut, 4-hour cap, first-touch
//!    attribution), ported so replay can rewrite a day's
//!    `iceberg/sessions/data/started_at_day=<day>/sessions-<day>.parquet`
//!    without a DuckDB dependency. The two implementations must stay in
//!    sync; the fixture tests here mirror that module's fixtures.
//! 6. **Idempotent output** — every object key is a pure function of its
//!    inputs (no UUIDs), so re-running a range overwrites the same keys
//!    instead of stacking duplicates next to them.
//! 7. **Checkpointing** — a JSON checkpoint records each completed source
//!    file and each sessionized day, saved atomically after every unit, so an
//!    interrupted replay resumes where it stopped. `--force` ignores it.
//!
//! Output layout (bucket-relative, under the same `TRACE_S3_PREFIX` as the
//! live flusher):
//!
//! ```text
//! <prefix>/events/<type>/date=YYYY-MM-DD/hour=HH/replay-raw-YYYYMMDD-HH.parquet
//! <prefix>/iceberg/sessions/data/started_at_day=YYYY-MM-DD/sessions-YYYY-MM-DD.parquet
//! ```
//!
//! The events half lands under `events/` — the glob every analytics view and
//! report reads (`docs/analytics/event_schema_versions.md`) — with the
//! `<type>/date=/hour=` directory shape that document records for the current
//! flusher generation. The sessions half is byte-for-byte the materializer's
//! key scheme, so a replayed day replaces (not joins) the nightly
//! materialization. Because sessions are recomputed only from the raw files
//! in the current selection, a day is sessionized by default only when all
//! 24 of its hour files are present; `--allow-partial-days` overrides with
//! the caveat that the day's sessions then reflect only the selected hours.
//!
//! Replay never deletes its inputs: the raw files are the source of truth
//! and must survive to be replayed again.

use crate::normalizer::{NormalizationMapping, NormalizedData};
use crate::raw_log_parser::{Event, EventType, RawLogParser};
use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use chrono::{DateTime, NaiveDate, Utc};
use flate2::read::GzDecoder;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs::{self, File};
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use tracing::{info, warn};

// ---------------------------------------------------------------------------
// Hour keys and range selection
// ---------------------------------------------------------------------------

/// One hour bucket — the unit of range selection, checkpointing, and output
/// partitioning. Derives `Ord` on (date, hour) so ranges sort chronologically.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct HourKey {
    pub date: NaiveDate,
    pub hour: u32,
}

impl HourKey {
    /// Parse an hour bound as given on the CLI: `YYYYMMDD-HH`, or `YYYYMMDD`
    /// (a whole day — hour 00 for a start bound, 23 for an end bound).
    pub fn parse_bound(s: &str, is_end: bool) -> Result<Self> {
        let s = s.trim();
        let (date_part, hour_part) = match s.split_once('-') {
            Some((d, h)) => (d, Some(h)),
            None => (s, None),
        };

        if date_part.len() != 8 || !date_part.bytes().all(|b| b.is_ascii_digit()) {
            bail!("date must be YYYYMMDD, got {s:?}");
        }
        let date = NaiveDate::from_ymd_opt(
            date_part[0..4].parse()?,
            date_part[4..6].parse()?,
            date_part[6..8].parse()?,
        )
        .with_context(|| format!("invalid date {s:?}"))?;

        let hour = match hour_part {
            Some(h) => {
                if h.len() != 2 || !h.bytes().all(|b| b.is_ascii_digit()) {
                    bail!("hour must be HH, got {s:?}");
                }
                let hour: u32 = h.parse()?;
                if hour > 23 {
                    bail!("hour must be 00-23, got {s:?}");
                }
                hour
            }
            None => {
                if is_end {
                    23
                } else {
                    0
                }
            }
        };

        Ok(Self { date, hour })
    }

    /// The canonical stem for this hour: the collector's raw file name minus
    /// extensions (`raw-YYYYMMDD-HH`). Used for checkpoint keys and output
    /// file names, so a replay's outputs are named after their input.
    pub fn stem(&self) -> String {
        format!("raw-{}-{:02}", self.date.format("%Y%m%d"), self.hour)
    }

    /// Hive partition date value (`YYYY-MM-DD`)
    pub fn date_string(&self) -> String {
        self.date.format("%Y-%m-%d").to_string()
    }

    /// Hive partition hour value (`HH`)
    pub fn hour_string(&self) -> String {
        format!("{:02}", self.hour)
    }
}

/// Which form a raw file was found in. When the same hour exists in more than
/// one form (e.g. both `raw-X.jsonl` and its rotation rename), the live
/// collector's chronology decides: `.ready` (rotated, sealed) beats a still
/// open `.jsonl`, which beats a compressed `.jsonl.gz` archive.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum RawForm {
    Gz,
    Plain,
    Ready,
}

/// Recognized raw file name → (hour, form). Mirrors the live flusher's
/// `parse_raw_hour_key` filename contract: `raw-YYYYMMDD-HH` with
/// `.jsonl`, `.jsonl.ready`, or `.jsonl.gz` (the archive form replay usually
/// reads).
fn parse_raw_filename(filename: &str) -> Option<(HourKey, RawForm)> {
    // Longest suffix first so ".jsonl" doesn't shadow ".jsonl.ready"/".jsonl.gz"
    const FORMS: [(&str, RawForm); 3] = [
        (".jsonl.ready", RawForm::Ready),
        (".jsonl.gz", RawForm::Gz),
        (".jsonl", RawForm::Plain),
    ];
    let (base, form) = FORMS
        .iter()
        .find_map(|(suffix, form)| filename.strip_suffix(suffix).map(|b| (b, *form)))?;

    let rest = base.strip_prefix("raw-")?;
    let (date_part, hour_part) = rest.split_once('-')?;
    if date_part.len() != 8 || hour_part.len() != 2 {
        return None;
    }
    if !date_part.bytes().all(|b| b.is_ascii_digit())
        || !hour_part.bytes().all(|b| b.is_ascii_digit())
    {
        return None;
    }
    let date = NaiveDate::from_ymd_opt(
        date_part[0..4].parse().ok()?,
        date_part[4..6].parse().ok()?,
        date_part[6..8].parse().ok()?,
    )?;
    let hour: u32 = hour_part.parse().ok()?;
    if hour > 23 {
        return None;
    }
    Some((HourKey { date, hour }, form))
}

/// The raw files under `dir` whose hour lies in the inclusive range
/// `[from, to]`, one per hour (highest-priority form wins), sorted by hour.
pub fn select_raw_files(dir: &Path, from: HourKey, to: HourKey) -> Result<Vec<(HourKey, PathBuf)>> {
    if from > to {
        bail!("range is empty: from {from:?} is after to {to:?}");
    }

    let mut best: HashMap<HourKey, (RawForm, PathBuf)> = HashMap::new();
    let entries =
        fs::read_dir(dir).with_context(|| format!("cannot read raw dir {}", dir.display()))?;
    for entry in entries {
        let entry = entry.with_context(|| "cannot read raw dir entry")?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let Some(filename) = path.file_name().and_then(|n| n.to_str()) else {
            continue;
        };
        let Some((hour, form)) = parse_raw_filename(filename) else {
            continue;
        };
        if hour < from || hour > to {
            continue;
        }
        match best.get(&hour) {
            Some((seen, _)) if *seen >= form => {}
            _ => {
                best.insert(hour, (form, path));
            }
        }
    }

    let mut selection: Vec<(HourKey, PathBuf)> = best
        .into_iter()
        .map(|(hour, (_, path))| (hour, path))
        .collect();
    selection.sort_by_key(|(hour, _)| *hour);
    Ok(selection)
}

// ---------------------------------------------------------------------------
// Normalization + asset extraction
// ---------------------------------------------------------------------------

/// The canonical columns one replayed event carries in addition to its parsed
/// fields: the network the mapping detected, and the asset/campaign
/// identifiers it extracted from network-specific parameters. These are the
/// columns the live path leaves NULL until an enrichment pass fills them.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct NormalizedColumns {
    pub network: Option<String>,
    pub campaign_id: Option<String>,
    pub creative_id: Option<String>,
    pub headline: Option<String>,
    pub image_id: Option<String>,
    pub item_id: Option<String>,
}

/// Detect the network and extract canonical campaign/asset identifiers from
/// an event's params. Params themselves are never rewritten — the raw values
/// stay in the `params` map verbatim, and the extraction lands in the typed
/// columns. The `generic` mapping still harvests `utm_campaign`/`headline`
/// style params when no network is detected; only the `network` column
/// itself stays NULL rather than recording the meaningless `"unknown"`.
pub fn normalize_event(event: &Event, mapping: &NormalizationMapping) -> NormalizedColumns {
    let data = NormalizedData::from_params(&event.params, mapping);
    NormalizedColumns {
        network: if data.network == "unknown" {
            None
        } else {
            Some(data.network)
        },
        campaign_id: data.campaign_id,
        creative_id: data.creative_id,
        headline: data.headline,
        image_id: data.image_id,
        item_id: data.item_id,
    }
}

/// A parsed event plus the columns normalization extracted from it — one row
/// of replayed `ad_events`.
#[derive(Debug, Clone)]
pub struct ReplayEventRecord {
    pub event: Event,
    pub normalized: NormalizedColumns,
}

/// Parse one raw log file into replay records: every line through
/// [`RawLogParser`], then per-file impression dedup, then normalization.
/// Unparseable lines are counted and skipped (they stay in the raw log for a
/// future replay with better logic — the collector-api contract).
/// Gzipped archives are transparently decompressed.
pub fn parse_replay_file(
    path: &Path,
    mapping: &NormalizationMapping,
) -> Result<(Vec<ReplayEventRecord>, usize)> {
    let file = File::open(path).with_context(|| format!("cannot open {}", path.display()))?;
    let reader: Box<dyn BufRead> = if path.extension().map(|e| e == "gz").unwrap_or(false) {
        Box::new(BufReader::new(GzDecoder::new(file)))
    } else {
        Box::new(BufReader::new(file))
    };

    let mut events = Vec::new();
    let mut errors = 0;
    for line in reader.lines() {
        let line = line.with_context(|| format!("read error in {}", path.display()))?;
        match RawLogParser::parse_line(&line) {
            Ok(event) => events.push(event),
            Err(e) => {
                errors += 1;
                if errors <= 10 {
                    warn!("replay: unparseable line in {}: {}", path.display(), e);
                }
            }
        }
    }

    let (events, dropped) = dedupe_by_imp_id(events);
    if dropped > 0 {
        info!(
            "replay: dropped {} duplicate imp_id sends in {}",
            dropped,
            path.display()
        );
    }

    let records = events
        .into_iter()
        .map(|event| {
            let normalized = normalize_event(&event, mapping);
            ReplayEventRecord { event, normalized }
        })
        .collect();
    Ok((records, errors))
}

/// Collapse duplicate sends carrying the same `imp_id` within one file
/// (beacon replay, prefetch double-fire, postback retry): first arrival
/// wins. Keyed on the param rather than the event type so it behaves the
/// same regardless of how the sender typed the event.
fn dedupe_by_imp_id(events: Vec<Event>) -> (Vec<Event>, usize) {
    let mut seen: HashSet<String> = HashSet::new();
    let before = events.len();
    let mut kept: Vec<Event> = Vec::new();
    for event in events {
        let first = if event.event_type != EventType::Impression {
            true
        } else {
            match event.params.get("imp_id") {
                Some(id) => seen.insert(id.clone()),
                None => true,
            }
        };
        if first {
            kept.push(event);
        }
    }
    let dropped = before - kept.len();
    (kept, dropped)
}

// ---------------------------------------------------------------------------
// Deterministic output keys
// ---------------------------------------------------------------------------

/// Bucket-relative key for one replayed hour's events of one type — a pure
/// function of (prefix, type, hour), so re-running a range overwrites the
/// same objects instead of writing new ones beside them.
pub fn events_object_key(prefix: &str, event_type: &str, hour: &HourKey) -> String {
    format!(
        "{}/events/{}/date={}/hour={}/replay-{}.parquet",
        prefix.trim_end_matches('/'),
        event_type,
        hour.date_string(),
        hour.hour_string(),
        hour.stem()
    )
}

/// Bucket-relative key for a day's materialized sessions — must match
/// `sessions_object_key` in `analytics/src/session_materializer.rs` exactly,
/// so a replayed day replaces the nightly materialization of that day
/// instead of joining it.
pub fn sessions_object_key(prefix: &str, day: NaiveDate) -> String {
    format!(
        "{}/iceberg/sessions/data/started_at_day={}/sessions-{}.parquet",
        prefix.trim_end_matches('/'),
        day.format("%Y-%m-%d"),
        day.format("%Y-%m-%d")
    )
}

// ---------------------------------------------------------------------------
// Checkpoint
// ---------------------------------------------------------------------------

/// Durable record of what a replay already completed, so an interrupted run
/// resumes instead of redoing (and re-uploading) finished units. Stored as
/// one JSON file, rewritten atomically after every completed unit.
///
/// Two maps, keyed by the units of work:
/// - `completed` — per raw-file stem (hour): which output keys landed, row
///   and parse-error counts. Present ⇒ skip the file on the next run.
/// - `sessions` — per day: which stems the sessionization consumed. A day is
///   skipped only when the recorded stems are a superset of the current
///   selection's stems for that day (a wider later selection must recompute).
#[derive(Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct Checkpoint {
    pub version: u32,
    #[serde(default)]
    pub completed: BTreeMap<String, CompletedFile>,
    #[serde(default)]
    pub sessions: BTreeMap<String, CompletedSessions>,
}

/// What the checkpoint records about one finished raw file
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CompletedFile {
    pub keys: Vec<String>,
    pub rows: usize,
    pub parse_errors: usize,
    pub completed_at: DateTime<Utc>,
}

/// What the checkpoint records about one sessionized day
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CompletedSessions {
    pub stems: Vec<String>,
    pub rows: usize,
    pub completed_at: DateTime<Utc>,
}

impl Checkpoint {
    pub const VERSION: u32 = 1;

    /// Load from `path`. A missing file is a fresh checkpoint; a corrupt one
    /// is warned about and treated as fresh — safe because output keys are
    /// deterministic, so redoing a unit overwrites rather than duplicates.
    pub fn load(path: &Path) -> Self {
        match fs::read_to_string(path) {
            Ok(content) => match serde_json::from_str::<Checkpoint>(&content) {
                Ok(cp) if cp.version == Self::VERSION => cp,
                Ok(cp) => {
                    warn!(
                        "replay: checkpoint {} has version {}, expected {}; starting fresh",
                        path.display(),
                        cp.version,
                        Self::VERSION
                    );
                    Self::default()
                }
                Err(e) => {
                    warn!(
                        "replay: checkpoint {} is corrupt ({}), starting fresh",
                        path.display(),
                        e
                    );
                    Self::default()
                }
            },
            Err(_) => Self::default(),
        }
    }

    /// Write atomically (temp file + rename) so a crash mid-save can never
    /// leave a truncated checkpoint.
    pub fn save(&self, path: &Path) -> Result<()> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("cannot create {}", parent.display()))?;
        }
        let tmp = path.with_extension("json.tmp");
        let content = serde_json::to_string_pretty(self).context("serialize checkpoint")?;
        fs::write(&tmp, content).with_context(|| format!("cannot write {}", tmp.display()))?;
        fs::rename(&tmp, path)
            .with_context(|| format!("cannot finalize checkpoint at {}", path.display()))?;
        Ok(())
    }

    pub fn is_file_complete(&self, stem: &str) -> bool {
        self.completed.contains_key(stem)
    }

    /// True when this checkpoint already sessionized `day` from at least the
    /// given stems (superset counts: more previously-done hours only make
    /// the recorded result more complete).
    pub fn sessions_covered(&self, day: NaiveDate, stems: &[String]) -> bool {
        match self.sessions.get(&day.format("%Y-%m-%d").to_string()) {
            Some(done) => stems.iter().all(|s| done.stems.contains(s)),
            None => false,
        }
    }

    pub fn record_file(&mut self, stem: &str, record: CompletedFile) {
        self.version = Self::VERSION;
        self.completed.insert(stem.to_string(), record);
    }

    pub fn record_sessions(&mut self, day: NaiveDate, record: CompletedSessions) {
        self.version = Self::VERSION;
        self.sessions
            .insert(day.format("%Y-%m-%d").to_string(), record);
    }
}

// ---------------------------------------------------------------------------
// Sessionization
// ---------------------------------------------------------------------------

/// Gap thresholds, mirroring `SessionConfig::default()` in
/// `analytics/src/session_stitcher.rs` — the values the authoritative
/// materializer runs with.
#[derive(Debug, Clone, Copy)]
pub struct SessionConfig {
    pub timeout_minutes: i64,
    pub max_session_hours: i64,
}

impl Default for SessionConfig {
    fn default() -> Self {
        Self {
            timeout_minutes: 30,
            max_session_hours: 4,
        }
    }
}

/// Event types that mark a session as converted — the same set the
/// materializer's SQL treats alongside `type = 'conversion'`.
const CONVERSION_TYPES: [EventType; 3] = [
    EventType::Conversion,
    EventType::Purchase,
    EventType::Signup,
];

/// One row of the `trace.sessions` DDL (`analytics/schemas/sessions_iceberg.sql`),
/// column for column, in DDL order.
#[derive(Debug, Clone, PartialEq)]
pub struct SessionRow {
    pub session_id: String,
    pub user_id: Option<String>,
    pub started_at: DateTime<Utc>,
    pub ended_at: DateTime<Utc>,
    pub pageviews: i32,
    pub clicks: i32,
    pub scrolls: i32,
    pub dwells: i32,
    pub event_count: i32,
    pub entry_url: String,
    pub exit_url: String,
    pub network: Option<String>,
    pub campaign_id: Option<String>,
    pub campaign_name: Option<String>,
    pub creative_id: Option<String>,
    pub headline: Option<String>,
    pub converted: bool,
    pub conversion_value: f64,
    pub device_type: Option<String>,
    pub device_os: Option<String>,
    pub referrer: Option<String>,
    pub duration_seconds: i32,
    pub bounce: bool,
    pub depth: i32,
}

/// Closed-open UTC window of a day, `[day 00:00Z, day+1 00:00Z)` — the
/// materializer's `day_window`.
fn day_window(day: NaiveDate) -> (DateTime<Utc>, DateTime<Utc>) {
    let start = day
        .and_hms_opt(0, 0, 0)
        .expect("midnight is a valid time")
        .and_utc();
    let end = day
        .succ_opt()
        .expect("every date has a successor")
        .and_hms_opt(0, 0, 0)
        .expect("midnight is a valid time")
        .and_utc();
    (start, end)
}

/// Sessionize one UTC day's events with the materializer's rules:
///
/// - only events with a session id inside the day window count;
/// - a gap longer than `timeout_minutes` splits a new session;
/// - sessions longer than `max_session_hours` are dropped entirely
///   (the SQL's `HAVING`);
/// - attribution columns are first-touch (value at the earliest event,
///   skipping NULLs, like DuckDB's `arg_min`);
/// - the id is day-qualified (`<sid>_<YYYYMMDD of first event>_<seq>`) so
///   sessions cut at midnight get distinct ids in distinct partitions.
///
/// Note on `dwells`: the materializer's SQL counts the literal
/// `type = 'dwell'`, a string the parser never emits (it maps dwell
/// heartbeats to `heartbeat`). Replay matches the SQL rather than "fixing"
/// it, so the two writers of `sessions-<day>.parquet` cannot disagree; the
/// quirk is flagged in `docs/notes/raw-log-replay.md`.
pub fn sessionize_day(
    day: NaiveDate,
    records: &[ReplayEventRecord],
    config: &SessionConfig,
) -> Vec<SessionRow> {
    let (window_start, window_end) = day_window(day);
    let timeout_secs = config.timeout_minutes * 60;
    let max_session_secs = config.max_session_hours * 3600;

    // Partition by session id, preserving arrival order within each
    let mut by_sid: HashMap<&str, Vec<&ReplayEventRecord>> = HashMap::new();
    for record in records {
        let Some(sid) = record.event.session_id.as_deref() else {
            continue;
        };
        let ts = record.event.ts;
        if ts < window_start || ts >= window_end {
            continue;
        }
        by_sid.entry(sid).or_default().push(record);
    }

    let mut rows = Vec::new();
    for (sid, mut group) in by_sid {
        group.sort_by_key(|r| r.event.ts);

        // Gap-split into segments; seq counts segments from 1
        let mut segments: Vec<Vec<&ReplayEventRecord>> = Vec::new();
        let mut current: Vec<&ReplayEventRecord> = Vec::new();
        let mut prev_ts: Option<DateTime<Utc>> = None;
        for record in group {
            let ts = record.event.ts;
            if let Some(prev) = prev_ts {
                if (ts - prev).num_seconds() > timeout_secs {
                    segments.push(std::mem::take(&mut current));
                }
            }
            prev_ts = Some(ts);
            current.push(record);
        }
        segments.push(current);

        for (idx, segment) in segments.into_iter().enumerate() {
            let started_at = segment[0].event.ts;
            let ended_at = segment[segment.len() - 1].event.ts;
            let duration = (ended_at - started_at).num_seconds();
            if duration > max_session_secs {
                continue;
            }

            let is_conversion =
                |r: &ReplayEventRecord| CONVERSION_TYPES.contains(&r.event.event_type);
            let conversion_value: f64 = segment
                .iter()
                .filter(|r| is_conversion(r))
                .filter_map(|r| r.event.params.get("revenue"))
                .filter_map(|v| v.parse::<f64>().ok())
                .sum();

            let count =
                |t: EventType| segment.iter().filter(|r| r.event.event_type == t).count() as i32;
            let first_touch =
                |f: fn(&ReplayEventRecord) -> Option<String>| segment.iter().find_map(|r| f(r));

            rows.push(SessionRow {
                session_id: format!("{}_{}_{}", sid, started_at.format("%Y%m%d"), idx + 1),
                user_id: first_touch(|r| r.event.user_id.clone()),
                started_at,
                ended_at,
                pageviews: count(EventType::Pageview),
                clicks: count(EventType::Click),
                scrolls: count(EventType::Scroll),
                dwells: segment
                    .iter()
                    .filter(|r| r.event.event_type.as_str() == "dwell")
                    .count() as i32,
                event_count: segment.len() as i32,
                entry_url: segment[0].event.url.clone(),
                exit_url: segment[segment.len() - 1].event.url.clone(),
                network: first_touch(|r| r.normalized.network.clone()),
                campaign_id: first_touch(|r| r.normalized.campaign_id.clone()),
                // No mapping produces campaign names; enrichment owns that column
                campaign_name: None,
                creative_id: first_touch(|r| r.normalized.creative_id.clone()),
                headline: first_touch(|r| r.normalized.headline.clone()),
                converted: segment.iter().any(|r| is_conversion(r)),
                conversion_value,
                // Device fields are enrichment-owned; raw events carry none
                device_type: None,
                device_os: None,
                referrer: first_touch(|r| r.event.referer.clone()),
                duration_seconds: duration as i32,
                bounce: segment.len() == 1,
                depth: segment
                    .iter()
                    .map(|r| r.event.url.as_str())
                    .collect::<HashSet<_>>()
                    .len() as i32,
            });
        }
    }

    rows.sort_by(|a, b| a.session_id.cmp(&b.session_id));
    rows
}

// ---------------------------------------------------------------------------
// Parquet serialization
// ---------------------------------------------------------------------------

/// Replay events → in-memory Parquet with the current flusher event schema
/// (the EV3 column set of `docs/analytics/event_schema_versions.md`, which
/// `analytics/src/events_compat.rs` pins against the `trace.ad_events` DDL).
/// Param entries are written in sorted key order so the encoded map never
/// depends on HashMap iteration order.
pub fn events_to_parquet(records: &[ReplayEventRecord]) -> Result<Vec<u8>> {
    use arrow::array::{
        BooleanArray, Float64Array, Int64Array, MapArray, StringArray, TimestampMillisecondArray,
    };
    use arrow::buffer::OffsetBuffer;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::arrow_writer::ArrowWriter;
    use parquet::file::properties::WriterProperties;
    use std::sync::Arc;

    let n = records.len();
    let timestamps: Vec<i64> = records
        .iter()
        .map(|r| r.event.ts.timestamp_millis())
        .collect();
    let ips: Vec<Option<String>> = records.iter().map(|r| r.event.ip.clone()).collect();
    let uas: Vec<Option<String>> = records.iter().map(|r| r.event.ua.clone()).collect();
    let urls: Vec<String> = records.iter().map(|r| r.event.url.clone()).collect();
    let types: Vec<String> = records
        .iter()
        .map(|r| r.event.event_type.as_str().to_string())
        .collect();
    let session_ids: Vec<Option<String>> =
        records.iter().map(|r| r.event.session_id.clone()).collect();
    let user_ids: Vec<Option<String>> = records.iter().map(|r| r.event.user_id.clone()).collect();
    let cookie_ids: Vec<Option<String>> =
        records.iter().map(|r| r.event.cookie_id.clone()).collect();
    let networks: Vec<Option<String>> = records
        .iter()
        .map(|r| r.normalized.network.clone())
        .collect();
    let campaign_ids: Vec<Option<String>> = records
        .iter()
        .map(|r| r.normalized.campaign_id.clone())
        .collect();
    let creative_ids: Vec<Option<String>> = records
        .iter()
        .map(|r| r.normalized.creative_id.clone())
        .collect();
    let headlines: Vec<Option<String>> = records
        .iter()
        .map(|r| r.normalized.headline.clone())
        .collect();
    let image_ids: Vec<Option<String>> = records
        .iter()
        .map(|r| r.normalized.image_id.clone())
        .collect();
    let item_ids: Vec<Option<String>> = records
        .iter()
        .map(|r| r.normalized.item_id.clone())
        .collect();
    let referrers: Vec<Option<String>> = records.iter().map(|r| r.event.referer.clone()).collect();
    let referrer_networks: Vec<Option<String>> = records
        .iter()
        .map(|r| r.event.referrer_network.clone())
        .collect();

    // Scroll depth arrives as a `scroll_depth` param on scroll events; the
    // typed column is promotion-only (non-scroll events stay NULL) with the
    // same 0-100 bounds as the enriched path.
    let scroll_depth_pcts: Vec<Option<i64>> = records
        .iter()
        .map(|r| {
            if r.event.event_type != EventType::Scroll {
                return None;
            }
            r.event
                .params
                .get("scroll_depth")
                .and_then(|v| v.parse::<i64>().ok())
                .filter(|&v| (0..=100).contains(&v))
        })
        .collect();

    // Enrichment-owned columns replay does not fill
    let campaign_names: Vec<Option<String>> = vec![None; n];
    let attribution_campaign_ids: Vec<Option<String>> = vec![None; n];
    let attribution_creative_ids: Vec<Option<String>> = vec![None; n];
    let attribution_touches: Vec<Option<i64>> = vec![None; n];
    let attribution_days_to_convert: Vec<Option<i64>> = vec![None; n];
    let device_types: Vec<Option<String>> = vec![None; n];
    let device_oss: Vec<Option<String>> = vec![None; n];
    let device_browsers: Vec<Option<String>> = vec![None; n];
    let scroll_time_mss: Vec<Option<i64>> = vec![None; n];
    let dwell_time_mss: Vec<Option<i64>> = vec![None; n];
    let dwell_visible_pcts: Vec<Option<i64>> = vec![None; n];
    let viewport_widths: Vec<Option<i64>> = vec![None; n];
    let viewport_heights: Vec<Option<i64>> = vec![None; n];
    let quality_scores: Vec<Option<f64>> = vec![None; n];
    let bot_probabilities: Vec<Option<f64>> = vec![None; n];
    let fraud_scores: Vec<Option<f64>> = vec![None; n];
    let is_valids: Vec<Option<bool>> = vec![None; n];
    let is_verifieds: Vec<Option<bool>> = vec![None; n];
    let validation_reasons: Vec<Option<String>> = vec![None; n];
    let enriched_ats: Vec<Option<i64>> = vec![None; n];
    let enrichment_versions: Vec<Option<String>> = vec![None; n];

    // params as a MapArray, entries in sorted key order for byte determinism
    let mut all_keys = Vec::new();
    let mut all_values = Vec::new();
    let mut offsets = vec![0i32];
    for record in records {
        let mut pairs: Vec<(&String, &String)> = record.event.params.iter().collect();
        pairs.sort_by(|a, b| a.0.cmp(b.0));
        for (key, value) in pairs {
            all_keys.push(key.clone());
            all_values.push(value.clone());
        }
        offsets.push(all_keys.len() as i32);
    }
    let entries_field = || {
        vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("value", DataType::Utf8, false),
        ]
    };
    let params_array = MapArray::new(
        Arc::new(Field::new(
            "entries",
            DataType::Struct(entries_field().into()),
            false,
        )),
        OffsetBuffer::new(offsets.into()),
        arrow::array::StructArray::new(
            entries_field().into(),
            vec![
                Arc::new(StringArray::from(all_keys)),
                Arc::new(StringArray::from(all_values)),
            ],
            None,
        ),
        None,
        false,
    );

    let ts_type = DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None);
    let schema = Schema::new(vec![
        Field::new("ts", ts_type.clone(), false),
        Field::new("ip", DataType::Utf8, true),
        Field::new("ua", DataType::Utf8, true),
        Field::new("url", DataType::Utf8, false),
        Field::new("type", DataType::Utf8, false),
        Field::new("session_id", DataType::Utf8, true),
        Field::new("user_id", DataType::Utf8, true),
        Field::new("cookie_id", DataType::Utf8, true),
        Field::new("network", DataType::Utf8, true),
        Field::new("campaign_id", DataType::Utf8, true),
        Field::new("campaign_name", DataType::Utf8, true),
        Field::new("creative_id", DataType::Utf8, true),
        Field::new("headline", DataType::Utf8, true),
        Field::new("image_id", DataType::Utf8, true),
        Field::new("item_id", DataType::Utf8, true),
        Field::new("referrer", DataType::Utf8, true),
        Field::new("referrer_network", DataType::Utf8, true),
        Field::new("attribution_campaign_id", DataType::Utf8, true),
        Field::new("attribution_creative_id", DataType::Utf8, true),
        Field::new("attribution_touches", DataType::Int64, true),
        Field::new("attribution_days_to_convert", DataType::Int64, true),
        Field::new("device_type", DataType::Utf8, true),
        Field::new("device_os", DataType::Utf8, true),
        Field::new("device_browser", DataType::Utf8, true),
        Field::new("scroll_depth_pct", DataType::Int64, true),
        Field::new("scroll_time_ms", DataType::Int64, true),
        Field::new("dwell_time_ms", DataType::Int64, true),
        Field::new("dwell_visible_pct", DataType::Int64, true),
        Field::new("viewport_width", DataType::Int64, true),
        Field::new("viewport_height", DataType::Int64, true),
        Field::new("quality_score", DataType::Float64, true),
        Field::new("bot_probability", DataType::Float64, true),
        Field::new("fraud_score", DataType::Float64, true),
        Field::new("is_valid", DataType::Boolean, true),
        Field::new("is_verified", DataType::Boolean, true),
        Field::new("validation_reason", DataType::Utf8, true),
        Field::new("enriched_at", ts_type, true),
        Field::new("enrichment_version", DataType::Utf8, true),
        Field::new(
            "params",
            DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(entries_field().into()),
                    false,
                )),
                false,
            ),
            true,
        ),
    ]);

    let batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(TimestampMillisecondArray::from(timestamps)),
            Arc::new(StringArray::from(ips)),
            Arc::new(StringArray::from(uas)),
            Arc::new(StringArray::from(urls)),
            Arc::new(StringArray::from(types)),
            Arc::new(StringArray::from(session_ids)),
            Arc::new(StringArray::from(user_ids)),
            Arc::new(StringArray::from(cookie_ids)),
            Arc::new(StringArray::from(networks)),
            Arc::new(StringArray::from(campaign_ids)),
            Arc::new(StringArray::from(campaign_names)),
            Arc::new(StringArray::from(creative_ids)),
            Arc::new(StringArray::from(headlines)),
            Arc::new(StringArray::from(image_ids)),
            Arc::new(StringArray::from(item_ids)),
            Arc::new(StringArray::from(referrers)),
            Arc::new(StringArray::from(referrer_networks)),
            Arc::new(StringArray::from(attribution_campaign_ids)),
            Arc::new(StringArray::from(attribution_creative_ids)),
            Arc::new(Int64Array::from(attribution_touches)),
            Arc::new(Int64Array::from(attribution_days_to_convert)),
            Arc::new(StringArray::from(device_types)),
            Arc::new(StringArray::from(device_oss)),
            Arc::new(StringArray::from(device_browsers)),
            Arc::new(Int64Array::from(scroll_depth_pcts)),
            Arc::new(Int64Array::from(scroll_time_mss)),
            Arc::new(Int64Array::from(dwell_time_mss)),
            Arc::new(Int64Array::from(dwell_visible_pcts)),
            Arc::new(Int64Array::from(viewport_widths)),
            Arc::new(Int64Array::from(viewport_heights)),
            Arc::new(Float64Array::from(quality_scores)),
            Arc::new(Float64Array::from(bot_probabilities)),
            Arc::new(Float64Array::from(fraud_scores)),
            Arc::new(BooleanArray::from(is_valids)),
            Arc::new(BooleanArray::from(is_verifieds)),
            Arc::new(StringArray::from(validation_reasons)),
            Arc::new(TimestampMillisecondArray::from(enriched_ats)),
            Arc::new(StringArray::from(enrichment_versions)),
            Arc::new(params_array),
        ],
    )?;

    let mut buffer = Vec::new();
    let mut writer = ArrowWriter::try_new(
        &mut buffer,
        batch.schema(),
        Some(WriterProperties::builder().build()),
    )?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(buffer)
}

/// Session rows → in-memory Parquet with the `trace.sessions` DDL shape:
/// timestamps as micros (DuckDB TIMESTAMP), counts as INT32,
/// `conversion_value` as DOUBLE.
pub fn sessions_to_parquet(rows: &[SessionRow]) -> Result<Vec<u8>> {
    use arrow::array::{
        BooleanArray, Float64Array, Int32Array, StringArray, TimestampMicrosecondArray,
    };
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::arrow_writer::ArrowWriter;
    use parquet::file::properties::WriterProperties;
    use std::sync::Arc;

    let session_ids: Vec<String> = rows.iter().map(|r| r.session_id.clone()).collect();
    let user_ids: Vec<Option<String>> = rows.iter().map(|r| r.user_id.clone()).collect();
    let started: Vec<i64> = rows
        .iter()
        .map(|r| r.started_at.timestamp_micros())
        .collect();
    let ended: Vec<i64> = rows.iter().map(|r| r.ended_at.timestamp_micros()).collect();
    let pageviews: Vec<i32> = rows.iter().map(|r| r.pageviews).collect();
    let clicks: Vec<i32> = rows.iter().map(|r| r.clicks).collect();
    let scrolls: Vec<i32> = rows.iter().map(|r| r.scrolls).collect();
    let dwells: Vec<i32> = rows.iter().map(|r| r.dwells).collect();
    let event_counts: Vec<i32> = rows.iter().map(|r| r.event_count).collect();
    let entry_urls: Vec<String> = rows.iter().map(|r| r.entry_url.clone()).collect();
    let exit_urls: Vec<String> = rows.iter().map(|r| r.exit_url.clone()).collect();
    let networks: Vec<Option<String>> = rows.iter().map(|r| r.network.clone()).collect();
    let campaign_ids: Vec<Option<String>> = rows.iter().map(|r| r.campaign_id.clone()).collect();
    let campaign_names: Vec<Option<String>> =
        rows.iter().map(|r| r.campaign_name.clone()).collect();
    let creative_ids: Vec<Option<String>> = rows.iter().map(|r| r.creative_id.clone()).collect();
    let headlines: Vec<Option<String>> = rows.iter().map(|r| r.headline.clone()).collect();
    let converted: Vec<bool> = rows.iter().map(|r| r.converted).collect();
    let conversion_values: Vec<f64> = rows.iter().map(|r| r.conversion_value).collect();
    let device_types: Vec<Option<String>> = rows.iter().map(|r| r.device_type.clone()).collect();
    let device_oss: Vec<Option<String>> = rows.iter().map(|r| r.device_os.clone()).collect();
    let referrers: Vec<Option<String>> = rows.iter().map(|r| r.referrer.clone()).collect();
    let durations: Vec<i32> = rows.iter().map(|r| r.duration_seconds).collect();
    let bounces: Vec<bool> = rows.iter().map(|r| r.bounce).collect();
    let depths: Vec<i32> = rows.iter().map(|r| r.depth).collect();

    let ts_type = DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None);
    let schema = Schema::new(vec![
        Field::new("session_id", DataType::Utf8, false),
        Field::new("user_id", DataType::Utf8, true),
        Field::new("started_at", ts_type.clone(), false),
        Field::new("ended_at", ts_type, false),
        Field::new("pageviews", DataType::Int32, false),
        Field::new("clicks", DataType::Int32, false),
        Field::new("scrolls", DataType::Int32, false),
        Field::new("dwells", DataType::Int32, false),
        Field::new("event_count", DataType::Int32, false),
        Field::new("entry_url", DataType::Utf8, false),
        Field::new("exit_url", DataType::Utf8, false),
        Field::new("network", DataType::Utf8, true),
        Field::new("campaign_id", DataType::Utf8, true),
        Field::new("campaign_name", DataType::Utf8, true),
        Field::new("creative_id", DataType::Utf8, true),
        Field::new("headline", DataType::Utf8, true),
        Field::new("converted", DataType::Boolean, false),
        Field::new("conversion_value", DataType::Float64, false),
        Field::new("device_type", DataType::Utf8, true),
        Field::new("device_os", DataType::Utf8, true),
        Field::new("referrer", DataType::Utf8, true),
        Field::new("duration_seconds", DataType::Int32, false),
        Field::new("bounce", DataType::Boolean, false),
        Field::new("depth", DataType::Int32, false),
    ]);

    let batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(StringArray::from(session_ids)),
            Arc::new(StringArray::from(user_ids)),
            Arc::new(TimestampMicrosecondArray::from(started)),
            Arc::new(TimestampMicrosecondArray::from(ended)),
            Arc::new(Int32Array::from(pageviews)),
            Arc::new(Int32Array::from(clicks)),
            Arc::new(Int32Array::from(scrolls)),
            Arc::new(Int32Array::from(dwells)),
            Arc::new(Int32Array::from(event_counts)),
            Arc::new(StringArray::from(entry_urls)),
            Arc::new(StringArray::from(exit_urls)),
            Arc::new(StringArray::from(networks)),
            Arc::new(StringArray::from(campaign_ids)),
            Arc::new(StringArray::from(campaign_names)),
            Arc::new(StringArray::from(creative_ids)),
            Arc::new(StringArray::from(headlines)),
            Arc::new(BooleanArray::from(converted)),
            Arc::new(Float64Array::from(conversion_values)),
            Arc::new(StringArray::from(device_types)),
            Arc::new(StringArray::from(device_oss)),
            Arc::new(StringArray::from(referrers)),
            Arc::new(Int32Array::from(durations)),
            Arc::new(BooleanArray::from(bounces)),
            Arc::new(Int32Array::from(depths)),
        ],
    )?;

    let mut buffer = Vec::new();
    let mut writer = ArrowWriter::try_new(
        &mut buffer,
        batch.schema(),
        Some(WriterProperties::builder().build()),
    )?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(buffer)
}

// ---------------------------------------------------------------------------
// Orchestration
// ---------------------------------------------------------------------------

/// Where replay output lands. Implementations put an object at a
/// bucket-relative key; S3 PUTs are overwrite-idempotent, which is what
/// makes deterministic keys safe to re-upload.
#[async_trait]
pub trait ReplaySink: Send + Sync {
    async fn put(&self, key: &str, data: Vec<u8>) -> Result<()>;
}

/// Everything `run_replay` needs, mirroring the CLI flags.
#[derive(Clone)]
pub struct ReplayConfig {
    /// Directory holding the raw-*.jsonl[.gz|.ready] archive
    pub raw_dir: PathBuf,
    /// Inclusive hour range
    pub from: HourKey,
    pub to: HourKey,
    /// S3 key prefix (`TRACE_S3_PREFIX`)
    pub s3_prefix: String,
    /// Reprocess files/days the checkpoint already recorded
    pub force: bool,
    /// Parse and plan, but upload nothing and write no checkpoint
    pub dry_run: bool,
    /// Skip the sessionization stage entirely
    pub skip_sessions: bool,
    /// Sessionize days even when fewer than 24 hour files are selected
    pub allow_partial_days: bool,
    /// Checkpoint file path
    pub checkpoint_path: PathBuf,
    /// Network normalization mapping
    pub mapping: NormalizationMapping,
    /// Sessionization thresholds
    pub session_config: SessionConfig,
}

/// Why a day's sessionization was (or was not) performed
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum DayStatus {
    Written,
    SkippedCheckpoint,
    SkippedPartial { hours_present: usize },
    SkippedDisabled,
    Failed,
}

/// Outcome of one day's sessionization stage
#[derive(Debug, Clone, Serialize)]
pub struct DayOutcome {
    pub day: NaiveDate,
    pub status: DayStatus,
    /// Session rows written (0 unless Written)
    pub rows: usize,
}

/// Summary of one replay run.
#[derive(Debug, Default, Serialize)]
pub struct ReplayOutcome {
    pub files_selected: usize,
    pub files_processed: usize,
    pub files_skipped_checkpoint: usize,
    pub events_rows: usize,
    pub parse_errors: usize,
    pub upload_failures: usize,
    /// Every key uploaded this run (planned keys, on a dry run)
    pub output_keys: Vec<String>,
    pub sessions: Vec<DayOutcome>,
}

impl ReplayOutcome {
    /// A run succeeded when nothing failed to upload.
    pub fn ok(&self) -> bool {
        self.upload_failures == 0 && self.sessions.iter().all(|d| d.status != DayStatus::Failed)
    }
}

/// Run the full replay: select → parse → normalize → upload events →
/// sessionize days → upload sessions, checkpointing after each unit.
///
/// The run is resumable: files already recorded in the checkpoint (and not
/// `--force`d) are skipped, days whose sessionization stems are already
/// covered are skipped, and everything else proceeds in hour order. Output
/// keys are deterministic, so any unit that runs twice overwrites its own
/// previous output rather than duplicating it.
pub async fn run_replay(config: &ReplayConfig, sink: &dyn ReplaySink) -> Result<ReplayOutcome> {
    let selection = select_raw_files(&config.raw_dir, config.from, config.to)?;
    let mut outcome = ReplayOutcome {
        files_selected: selection.len(),
        ..Default::default()
    };
    let mut checkpoint = Checkpoint::load(&config.checkpoint_path);

    // Per-day selection for the sessions stage
    let mut by_day: BTreeMap<NaiveDate, Vec<(HourKey, PathBuf)>> = BTreeMap::new();
    for (hour, path) in &selection {
        by_day
            .entry(hour.date)
            .or_default()
            .push((*hour, path.clone()));
    }

    for (hour, path) in &selection {
        let stem = hour.stem();
        if !config.force && checkpoint.is_file_complete(&stem) {
            outcome.files_skipped_checkpoint += 1;
            info!("replay: {stem} already complete, skipping");
            continue;
        }

        let (records, errors) = parse_replay_file(path, &config.mapping)
            .with_context(|| format!("replay: cannot parse {}", path.display()))?;
        outcome.parse_errors += errors;

        // Group by event type — each type is its own partition
        let mut by_type: BTreeMap<String, Vec<&ReplayEventRecord>> = BTreeMap::new();
        for record in &records {
            by_type
                .entry(record.event.event_type.as_str().to_string())
                .or_default()
                .push(record);
        }

        let mut keys = Vec::new();
        let mut file_failed = false;
        for (event_type, group) in &by_type {
            let owned: Vec<ReplayEventRecord> = group.iter().map(|r| (*r).clone()).collect();
            let parquet =
                events_to_parquet(&owned).context("replay: event Parquet conversion failed")?;
            let key = events_object_key(&config.s3_prefix, event_type, hour);
            if config.dry_run {
                info!(
                    "replay (dry-run): would upload {key} ({} rows)",
                    group.len()
                );
                keys.push(key);
                continue;
            }
            match sink.put(&key, parquet).await {
                Ok(()) => {
                    info!("replay: uploaded {key} ({} rows)", group.len());
                    keys.push(key);
                }
                Err(e) => {
                    warn!("replay: upload failed for {key}: {e}");
                    outcome.upload_failures += 1;
                    file_failed = true;
                }
            }
        }

        if config.dry_run {
            outcome.files_processed += 1;
            outcome.events_rows += records.len();
            outcome.output_keys.extend(keys);
            continue;
        }

        if file_failed {
            // Not checkpointed — the next run retries this file whole
            continue;
        }

        outcome.files_processed += 1;
        outcome.events_rows += records.len();
        outcome.output_keys.extend(keys.clone());
        checkpoint.record_file(
            &stem,
            CompletedFile {
                keys,
                rows: records.len(),
                parse_errors: errors,
                completed_at: Utc::now(),
            },
        );
        checkpoint.save(&config.checkpoint_path)?;
    }

    if config.skip_sessions || config.dry_run {
        if config.dry_run && !config.skip_sessions {
            info!("replay (dry-run): sessions stage planned but not executed");
        }
        return Ok(outcome);
    }

    // Sessions: every day in the selected range, in order. The partial-day
    // guard below also protects days with no selected files at all — writing
    // an empty sessions-<day>.parquet from zero evidence would erase the
    // nightly materialization for raw hours this replay never saw.
    let mut day = config.from.date;
    while day <= config.to.date {
        let selection_for_day = by_day.get(&day).cloned().unwrap_or_default();
        let stems: Vec<String> = selection_for_day.iter().map(|(h, _)| h.stem()).collect();

        if !config.force && checkpoint.sessions_covered(day, &stems) {
            outcome.sessions.push(DayOutcome {
                day,
                status: DayStatus::SkippedCheckpoint,
                rows: 0,
            });
            day = next_day(day);
            continue;
        }

        if stems.len() < 24 && !config.allow_partial_days {
            warn!(
                "replay: {} has only {} of 24 hour files; pass --allow-partial-days to \
                 sessionize it from those hours alone (the day's sessions would then \
                 reflect only the selected hours) — skipping",
                day.format("%Y-%m-%d"),
                stems.len()
            );
            outcome.sessions.push(DayOutcome {
                day,
                status: DayStatus::SkippedPartial {
                    hours_present: stems.len(),
                },
                rows: 0,
            });
            day = next_day(day);
            continue;
        }

        let mut records = Vec::new();
        for (_, path) in &selection_for_day {
            let (day_records, errors) = parse_replay_file(path, &config.mapping)
                .with_context(|| format!("replay: cannot parse {}", path.display()))?;
            outcome.parse_errors += errors;
            records.extend(day_records);
        }
        let rows = sessionize_day(day, &records, &config.session_config);
        let parquet = sessions_to_parquet(&rows)?;
        let key = sessions_object_key(&config.s3_prefix, day);

        match sink.put(&key, parquet).await {
            Ok(()) => {
                info!("replay: uploaded {key} ({} sessions)", rows.len());
                outcome.output_keys.push(key.clone());
                outcome.sessions.push(DayOutcome {
                    day,
                    status: DayStatus::Written,
                    rows: rows.len(),
                });
                checkpoint.record_sessions(
                    day,
                    CompletedSessions {
                        stems,
                        rows: rows.len(),
                        completed_at: Utc::now(),
                    },
                );
                checkpoint.save(&config.checkpoint_path)?;
            }
            Err(e) => {
                warn!("replay: sessions upload failed for {key}: {e}");
                outcome.upload_failures += 1;
                outcome.sessions.push(DayOutcome {
                    day,
                    status: DayStatus::Failed,
                    rows: 0,
                });
            }
        }

        day = next_day(day);
    }

    Ok(outcome)
}

fn next_day(day: NaiveDate) -> NaiveDate {
    day.succ_opt().expect("every date has a successor")
}

/// The network mapping embedded at build time — the fallback when the CLI
/// is not given `--mapping`, so a replay never depends on a file that the
/// container image may not carry.
pub fn default_network_mapping() -> Result<NormalizationMapping> {
    NormalizationMapping::from_toml_str(include_str!("network_mapping.toml"))
        .context("embedded network_mapping.toml is invalid")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write as _;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;

    // ------------------------------------------------------------------
    // Fixtures
    // ------------------------------------------------------------------

    fn mapping() -> NormalizationMapping {
        default_network_mapping().unwrap()
    }

    /// Write one raw log file with the given lines under `dir`.
    fn write_raw(dir: &Path, stem: &str, lines: &[String]) -> PathBuf {
        let path = dir.join(format!("{stem}.jsonl"));
        let mut file = File::create(&path).unwrap();
        for line in lines {
            writeln!(file, "{line}").unwrap();
        }
        file.sync_all().unwrap();
        path
    }

    /// A raw GET line: query params become the event params.
    fn get_line(ts: &str, query: &str) -> String {
        format!(
            r#"{{"ts":"{ts}","method":"GET","path":"/p","headers":{{}},"query_params":"{query}","body":null}}"#
        )
    }

    /// A raw POST /e line carrying a form-encoded tag payload. The
    /// RawRequest body is a string field; the parser JSON-decodes it first
    /// and falls back to form-encoding, and this fixture stays valid under
    /// both decoders.
    fn post_line(ts: &str, body: &str) -> String {
        let body_json = serde_json::to_string(body).unwrap();
        format!(
            r#"{{"ts":"{ts}","method":"POST","path":"/e","headers":{{}},"query_params":null,"body":{body_json}}}"#
        )
    }

    fn hour(date: &str, h: u32) -> HourKey {
        HourKey {
            date: NaiveDate::parse_from_str(date, "%Y-%m-%d").unwrap(),
            hour: h,
        }
    }

    /// Sink that records puts; `fail_first` makes the first N puts error.
    struct MemorySink {
        puts: Mutex<Vec<(String, usize)>>,
        fail_first: AtomicUsize,
    }

    impl MemorySink {
        fn new() -> Self {
            Self {
                puts: Mutex::new(Vec::new()),
                fail_first: AtomicUsize::new(0),
            }
        }

        fn with_failures(n: usize) -> Self {
            Self {
                puts: Mutex::new(Vec::new()),
                fail_first: AtomicUsize::new(n),
            }
        }

        fn keys(&self) -> Vec<String> {
            self.puts
                .lock()
                .unwrap()
                .iter()
                .map(|(k, _)| k.clone())
                .collect()
        }
    }

    #[async_trait]
    impl ReplaySink for MemorySink {
        async fn put(&self, key: &str, data: Vec<u8>) -> Result<()> {
            let remaining = self
                .fail_first
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| {
                    if n > 0 {
                        Some(n - 1)
                    } else {
                        None
                    }
                });
            if let Ok(remaining) = remaining {
                anyhow::bail!(
                    "injected failure #{}, {} remaining",
                    remaining + 1,
                    remaining
                );
            }
            self.puts
                .lock()
                .unwrap()
                .push((key.to_string(), data.len()));
            Ok(())
        }
    }

    fn test_config(dir: &Path, cp: &Path) -> ReplayConfig {
        ReplayConfig {
            raw_dir: dir.to_path_buf(),
            from: HourKey::parse_bound("20260914", false).unwrap(),
            to: HourKey::parse_bound("20260914", true).unwrap(),
            s3_prefix: "trace-events".to_string(),
            force: false,
            dry_run: false,
            skip_sessions: true,
            allow_partial_days: false,
            checkpoint_path: cp.to_path_buf(),
            mapping: mapping(),
            session_config: SessionConfig::default(),
        }
    }

    // ------------------------------------------------------------------
    // Hour keys and selection
    // ------------------------------------------------------------------

    #[test]
    fn hour_key_parses_bounds() {
        let from = HourKey::parse_bound("20260914-05", false).unwrap();
        assert_eq!(from, hour("2026-09-14", 5));
        assert_eq!(from.stem(), "raw-20260914-05");
        assert_eq!(from.date_string(), "2026-09-14");
        assert_eq!(from.hour_string(), "05");

        // Date-only: hour 00 for start, 23 for end
        assert_eq!(
            HourKey::parse_bound("20260914", false).unwrap(),
            hour("2026-09-14", 0)
        );
        assert_eq!(
            HourKey::parse_bound("20260914", true).unwrap(),
            hour("2026-09-14", 23)
        );

        assert!(HourKey::parse_bound("20260914-24", false).is_err());
        assert!(HourKey::parse_bound("2026-9-4", false).is_err());
        assert!(HourKey::parse_bound("not-a-date", false).is_err());
        assert!(HourKey::parse_bound("20260931-00", false).is_err());
    }

    #[test]
    fn raw_filename_forms() {
        for name in [
            "raw-20260914-05.jsonl",
            "raw-20260914-05.jsonl.ready",
            "raw-20260914-05.jsonl.gz",
        ] {
            let (h, _) = parse_raw_filename(name).unwrap();
            assert_eq!(h, hour("2026-09-14", 5), "{name}");
        }
        for name in [
            "raw-20260914.jsonl",
            "raw-20260914-5.jsonl",
            "events-20260914-05.jsonl",
            "raw-20260914-05.jsonl.gz.tmp",
            "raw-20261301-05.jsonl",
        ] {
            assert!(parse_raw_filename(name).is_none(), "{name}");
        }
    }

    #[test]
    fn selection_filters_range_and_prefers_ready() {
        let dir = tempfile::tempdir().unwrap();
        write_raw(
            dir.path(),
            "raw-20260914-04",
            &[get_line("2026-09-14T04:00:00Z", "type=pageview")],
        );
        write_raw(
            dir.path(),
            "raw-20260914-05",
            &[get_line("2026-09-14T05:00:00Z", "type=pageview")],
        );
        // The same hour rotated: .ready outranks the still-open .jsonl
        write_raw(
            dir.path(),
            "raw-20260914-06",
            &[get_line("2026-09-14T06:00:00Z", "type=pageview")],
        );
        let ready = dir.path().join("raw-20260914-06.jsonl.ready");
        fs::rename(dir.path().join("raw-20260914-06.jsonl"), &ready).unwrap();
        // Out-of-range and unrecognized names are ignored
        write_raw(
            dir.path(),
            "raw-20260915-07",
            &[get_line("2026-09-15T07:00:00Z", "type=pageview")],
        );
        std::fs::write(dir.path().join("unrelated.txt"), "not a raw log").unwrap();

        let sel = select_raw_files(
            dir.path(),
            HourKey::parse_bound("20260914-05", false).unwrap(),
            HourKey::parse_bound("20260914-23", true).unwrap(),
        )
        .unwrap();

        let stems: Vec<String> = sel.iter().map(|(h, _)| h.stem()).collect();
        assert_eq!(stems, vec!["raw-20260914-05", "raw-20260914-06"]);
        assert_eq!(sel[1].1, ready, "rotated .ready form must win");

        assert!(select_raw_files(
            dir.path(),
            HourKey::parse_bound("20260916", false).unwrap(),
            HourKey::parse_bound("20260916", true).unwrap(),
        )
        .unwrap()
        .is_empty());

        // Reversed range is an error, not an implicit swap
        assert!(select_raw_files(
            dir.path(),
            HourKey::parse_bound("20260915", false).unwrap(),
            HourKey::parse_bound("20260914", true).unwrap(),
        )
        .is_err());
    }

    #[test]
    fn gz_raw_files_are_selected_and_parsed() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("raw-20260914-05.jsonl.gz");
        let mut gz =
            flate2::write::GzEncoder::new(File::create(&path).unwrap(), Default::default());
        writeln!(
            gz,
            "{}",
            get_line("2026-09-14T05:10:00Z", "type=pageview&sid=s1")
        )
        .unwrap();
        writeln!(
            gz,
            "{}",
            get_line("2026-09-14T05:20:00Z", "type=click&sid=s1")
        )
        .unwrap();
        gz.finish().unwrap();

        let sel =
            select_raw_files(dir.path(), hour("2026-09-14", 0), hour("2026-09-14", 23)).unwrap();
        assert_eq!(sel.len(), 1);
        assert_eq!(sel[0].1, path);

        let (records, errors) = parse_replay_file(&path, &mapping()).unwrap();
        assert_eq!(errors, 0);
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].event.event_type, EventType::Pageview);
        assert_eq!(records[1].event.event_type, EventType::Click);
    }

    // ------------------------------------------------------------------
    // Keys
    // ------------------------------------------------------------------

    #[test]
    fn object_keys_are_deterministic_and_exact() {
        let h = hour("2026-09-14", 5);
        assert_eq!(
            events_object_key("trace-events", "pageview", &h),
            "trace-events/events/pageview/date=2026-09-14/hour=05/replay-raw-20260914-05.parquet"
        );
        // A pure function of its inputs — same inputs, same key, no UUIDs
        assert_eq!(
            events_object_key("trace-events", "pageview", &h),
            events_object_key("trace-events", "pageview", &h)
        );
        // Trailing slash in the prefix is normalized away
        assert_eq!(
            events_object_key("trace-events/", "pageview", &h),
            events_object_key("trace-events", "pageview", &h)
        );

        // Must equal the analytics materializer's key scheme exactly
        // (analytics/src/session_materializer.rs, sessions_object_key)
        let day = NaiveDate::from_ymd_opt(2026, 9, 14).unwrap();
        assert_eq!(
            sessions_object_key("trace-events", day),
            "trace-events/iceberg/sessions/data/started_at_day=2026-09-14/sessions-2026-09-14.parquet"
        );
        assert_eq!(
            sessions_object_key("p/", day),
            sessions_object_key("p", day)
        );
    }

    // ------------------------------------------------------------------
    // Parsing, dedup, normalization
    // ------------------------------------------------------------------

    #[test]
    fn parse_normalizes_network_and_assets() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_raw(
            dir.path(),
            "raw-20260914-05",
            &[get_line(
                "2026-09-14T05:00:00Z",
                "type=pageview&sid=s1&utm_source=taboola&utm_campaign=camp1&tb_headline=Headline%20One&tb_image=img-1&tb_item=item-9",
            )],
        );

        let (records, errors) = parse_replay_file(&path, &mapping()).unwrap();
        assert_eq!(errors, 0);
        assert_eq!(records.len(), 1);

        let n = &records[0].normalized;
        assert_eq!(n.network.as_deref(), Some("taboola"));
        assert_eq!(n.campaign_id.as_deref(), Some("camp1"));
        assert_eq!(n.headline.as_deref(), Some("Headline One"));
        assert_eq!(n.image_id.as_deref(), Some("img-1"));
        assert_eq!(n.item_id.as_deref(), Some("item-9"));
        assert_eq!(n.creative_id.as_deref(), Some("img-1"));

        // Raw params survive verbatim alongside the typed columns
        assert_eq!(
            records[0]
                .event
                .params
                .get("utm_source")
                .map(String::as_str),
            Some("taboola")
        );
    }

    #[test]
    fn parse_counts_errors_and_keeps_good_lines() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_raw(
            dir.path(),
            "raw-20260914-05",
            &[
                "not json at all".to_string(),
                get_line("2026-09-14T05:00:00Z", "type=pageview"),
                r#"{"ts":"bad","method":"GET","path":"/p","headers":{},"query_params":null,"body":null}"#.to_string(),
            ],
        );

        let (records, errors) = parse_replay_file(&path, &mapping()).unwrap();
        assert_eq!(errors, 2);
        assert_eq!(records.len(), 1);
    }

    #[test]
    fn duplicate_imp_ids_collapse_per_file() {
        let dir = tempfile::tempdir().unwrap();
        let imp = |ts: &str, id: &str| {
            post_line(
                ts,
                &format!("type=impression&sid=s1&imp_id={id}&in_view_ms=1200"),
            )
        };
        let path = write_raw(
            dir.path(),
            "raw-20260914-05",
            &[
                imp("2026-09-14T05:00:00Z", "imp-a"),
                imp("2026-09-14T05:00:01Z", "imp-a"),
                imp("2026-09-14T05:00:02Z", "imp-b"),
                post_line(
                    "2026-09-14T05:00:03Z",
                    "type=impression&sid=s1&in_view_ms=900",
                ),
                post_line("2026-09-14T05:00:04Z", "type=click&sid=s1&imp_id=imp-a"),
            ],
        );

        let (records, errors) = parse_replay_file(&path, &mapping()).unwrap();
        assert_eq!(errors, 0);
        assert_eq!(
            records.len(),
            4,
            "impression dedup does not drop click events"
        );

        let imp_ids: Vec<&str> = records
            .iter()
            .filter(|record| record.event.event_type == EventType::Impression)
            .filter_map(|record| record.event.params.get("imp_id").map(String::as_str))
            .collect();
        assert_eq!(imp_ids, vec!["imp-a", "imp-b"]);
    }

    // ------------------------------------------------------------------
    // Event Parquet
    // ------------------------------------------------------------------

    /// Read a Parquet column back so tests assert on content, not just on
    /// successful conversion.
    fn read_parquet_batches(bytes: &[u8]) -> Vec<arrow::record_batch::RecordBatch> {
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
        let file = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(file.path(), bytes).unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(file.path()).unwrap())
            .unwrap()
            .build()
            .unwrap();
        reader.map(|b| b.unwrap()).collect()
    }

    /// Read a Parquet file's schema back — works for zero-row files, where
    /// the batch iterator yields nothing but the schema is still pinned.
    fn read_parquet_schema(bytes: &[u8]) -> arrow::datatypes::SchemaRef {
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
        let file = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(file.path(), bytes).unwrap();
        ParquetRecordBatchReaderBuilder::try_new(File::open(file.path()).unwrap())
            .unwrap()
            .schema()
            .clone()
    }

    #[test]
    fn events_parquet_carries_normalized_columns() {
        use arrow::array::{Array, MapArray, StringArray};

        let dir = tempfile::tempdir().unwrap();
        let path = write_raw(
            dir.path(),
            "raw-20260914-05",
            &[
                get_line(
                    "2026-09-14T05:00:00Z",
                    "type=pageview&sid=s1&utm_source=taboola&utm_campaign=camp1&tb_headline=Headline%20One&tb_image=img-1&tb_item=item-9",
                ),
                get_line("2026-09-14T05:01:00Z", "type=scroll&sid=s1&scroll_depth=75"),
                get_line("2026-09-14T05:02:00Z", "type=click&sid=s1"),
            ],
        );
        let (records, _) = parse_replay_file(&path, &mapping()).unwrap();
        let bytes = events_to_parquet(&records).unwrap();

        let batches = read_parquet_batches(&bytes);
        assert_eq!(batches.len(), 1);
        let batch = &batches[0];
        assert_eq!(batch.num_rows(), 3);

        let col = |name: &str| -> Vec<Option<String>> {
            batch
                .column_by_name(name)
                .unwrap_or_else(|| panic!("column {name} missing"))
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap_or_else(|| panic!("column {name} not Utf8"))
                .iter()
                .map(|v| v.map(|s| s.to_string()))
                .collect()
        };

        assert_eq!(
            col("network"),
            vec![Some("taboola".into()), None, None],
            "network lands only where detected"
        );
        assert_eq!(col("campaign_id"), vec![Some("camp1".into()), None, None]);
        assert_eq!(
            col("headline"),
            vec![Some("Headline One".into()), None, None]
        );
        assert_eq!(col("image_id"), vec![Some("img-1".into()), None, None]);
        assert_eq!(col("item_id"), vec![Some("item-9".into()), None, None]);
        assert_eq!(
            col("session_id"),
            vec![Some("s1".into()), Some("s1".into()), Some("s1".into())]
        );

        // Raw params survive verbatim in the map
        let params = batch
            .column_by_name("params")
            .unwrap()
            .as_any()
            .downcast_ref::<MapArray>()
            .unwrap();
        let entries = params.value(0);
        let keys = entries
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let values = entries
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let mut got = std::collections::HashMap::new();
        for i in 0..keys.len() {
            got.insert(keys.value(i).to_string(), values.value(i).to_string());
        }
        assert_eq!(got.get("utm_source").map(String::as_str), Some("taboola"));
        assert_eq!(
            got.get("tb_headline").map(String::as_str),
            Some("Headline One")
        );
    }

    /// The schema must stay the EV3 column set — membership and order —
    /// pinned the same way `analytics/src/events_compat.rs` pins it against
    /// the DDL, so replayed files read back through the same views.
    #[test]
    fn events_parquet_schema_is_ev3() {
        let bytes = events_to_parquet(&[]).unwrap();
        let schema = read_parquet_schema(&bytes);

        let expected: &[(&str, &str)] = &[
            ("ts", "Timestamp(Millisecond, None)"),
            ("ip", "Utf8"),
            ("ua", "Utf8"),
            ("url", "Utf8"),
            ("type", "Utf8"),
            ("session_id", "Utf8"),
            ("user_id", "Utf8"),
            ("cookie_id", "Utf8"),
            ("network", "Utf8"),
            ("campaign_id", "Utf8"),
            ("campaign_name", "Utf8"),
            ("creative_id", "Utf8"),
            ("headline", "Utf8"),
            ("image_id", "Utf8"),
            ("item_id", "Utf8"),
            ("referrer", "Utf8"),
            ("referrer_network", "Utf8"),
            ("attribution_campaign_id", "Utf8"),
            ("attribution_creative_id", "Utf8"),
            ("attribution_touches", "Int64"),
            ("attribution_days_to_convert", "Int64"),
            ("device_type", "Utf8"),
            ("device_os", "Utf8"),
            ("device_browser", "Utf8"),
            ("scroll_depth_pct", "Int64"),
            ("scroll_time_ms", "Int64"),
            ("dwell_time_ms", "Int64"),
            ("dwell_visible_pct", "Int64"),
            ("viewport_width", "Int64"),
            ("viewport_height", "Int64"),
            ("quality_score", "Float64"),
            ("bot_probability", "Float64"),
            ("fraud_score", "Float64"),
            ("is_valid", "Boolean"),
            ("is_verified", "Boolean"),
            ("validation_reason", "Utf8"),
            ("enriched_at", "Timestamp(Millisecond, None)"),
            ("enrichment_version", "Utf8"),
            ("params", "Map("),
        ];

        assert_eq!(
            schema.fields().len(),
            expected.len(),
            "EV3 column count changed"
        );
        for (i, (name, type_prefix)) in expected.iter().enumerate() {
            let field = schema.field(i);
            assert_eq!(field.name(), *name, "column {i}");
            let type_str = format!("{:?}", field.data_type());
            assert!(
                type_str.starts_with(type_prefix),
                "column {name}: unexpected type {type_str}"
            );
        }
    }

    /// Same input must produce the same bytes: param entries are written in
    /// sorted key order, so a re-run's overwrite is byte-stable too.
    #[test]
    fn events_parquet_is_byte_deterministic() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_raw(
            dir.path(),
            "raw-20260914-05",
            &[get_line(
                "2026-09-14T05:00:00Z",
                "type=pageview&utm_source=taboola&z=1&a=2",
            )],
        );
        let (records, _) = parse_replay_file(&path, &mapping()).unwrap();
        let first = events_to_parquet(&records).unwrap();
        let second = events_to_parquet(&records).unwrap();
        assert_eq!(first, second);
    }

    // ------------------------------------------------------------------
    // Sessionization
    // ------------------------------------------------------------------

    /// Build a sessionization input straight from raw lines (parse +
    /// normalize), the way `run_replay` feeds the stage.
    fn records_from_lines(lines: &[String]) -> Vec<ReplayEventRecord> {
        lines
            .iter()
            .map(|line| {
                let event = RawLogParser::parse_line(line).unwrap();
                let normalized = normalize_event(&event, &mapping());
                ReplayEventRecord { event, normalized }
            })
            .collect()
    }

    /// The same fixture shape as the materializer's end-to-end test
    /// (`test_day_materialization_end_to_end` in
    /// analytics/src/session_materializer.rs): one session split by a
    /// 70-minute gap, a bounce, and a next-day event the window excludes.
    #[test]
    fn sessionize_day_matches_materializer_semantics() {
        let lines = vec![
            get_line("2026-09-14T10:00:00Z", "type=pageview&sid=s1&uid=u1&utm_source=taboola&utm_campaign=camp1&tb_headline=Headline%20One"),
            get_line("2026-09-14T10:05:00Z", "type=click&sid=s1&uid=u1"),
            get_line("2026-09-14T10:12:00Z", "type=scroll&sid=s1&uid=u1&scroll_depth=40"),
            post_line("2026-09-14T10:20:00Z", "type=conversion&sid=s1&uid=u1&revenue=42.5"),
            // 70-minute gap: new session, its own first-touch attribution
            get_line("2026-09-14T11:30:00Z", "type=pageview&sid=s1&uid=u1&utm_source=outbrain&utm_campaign=camp2"),
            post_line("2026-09-14T11:31:00Z", "type=heartbeat&sid=s1&uid=u1"),
            // Bounce
            get_line("2026-09-14T12:00:00Z", "type=pageview&sid=s2"),
            // Next day: excluded by the day window
            get_line("2026-09-15T00:30:00Z", "type=pageview&sid=s3"),
        ];
        let records = records_from_lines(&lines);

        let rows = sessionize_day(
            NaiveDate::from_ymd_opt(2026, 9, 14).unwrap(),
            &records,
            &SessionConfig::default(),
        );

        assert_eq!(rows.len(), 3);
        let by_id: std::collections::HashMap<&str, &SessionRow> =
            rows.iter().map(|r| (r.session_id.as_str(), r)).collect();

        let s1a = by_id["s1_20260914_1"];
        assert_eq!(s1a.user_id.as_deref(), Some("u1"));
        assert_eq!(
            (
                s1a.pageviews,
                s1a.clicks,
                s1a.scrolls,
                s1a.dwells,
                s1a.event_count
            ),
            (1, 1, 1, 0, 4)
        );
        // The parser builds the URL string in HashMap iteration order (not
        // sorted at HEAD), so assert on membership rather than exact string
        assert!(s1a.entry_url.starts_with("/p?"));
        assert!(s1a.entry_url.contains("sid=s1"));
        assert!(s1a.entry_url.contains("utm_campaign=camp1"));
        // The segment's last event is the POST conversion
        assert!(s1a.exit_url.starts_with("/e?"));
        assert!(s1a.exit_url.contains("type=conversion"));
        assert_eq!(s1a.network.as_deref(), Some("taboola"));
        assert_eq!(s1a.campaign_id.as_deref(), Some("camp1"));
        assert_eq!(s1a.headline.as_deref(), Some("Headline One"));
        assert!(s1a.converted);
        assert!((s1a.conversion_value - 42.5).abs() < f64::EPSILON);
        assert_eq!(s1a.duration_seconds, 1200);
        assert!(!s1a.bounce);
        assert_eq!(s1a.depth, 4);

        let s1b = by_id["s1_20260914_2"];
        assert_eq!(s1b.network.as_deref(), Some("outbrain"));
        assert_eq!(s1b.campaign_id.as_deref(), Some("camp2"));
        assert_eq!(s1b.event_count, 2);
        assert!(!s1b.bounce);
        // dwells is 0: the materializer's SQL counts the literal 'dwell',
        // which the parser never emits — see sessionize_day's doc comment
        assert_eq!(s1b.dwells, 0);

        let s2 = by_id["s2_20260914_1"];
        assert!(s2.bounce);
        assert_eq!(s2.event_count, 1);
        assert_eq!(s2.depth, 1);
        assert_eq!(s2.conversion_value, 0.0);
        assert!(!s2.converted);
        assert_eq!(s2.user_id, None);
    }

    #[test]
    fn sessionize_day_drops_sessions_over_the_cap() {
        // One continuous chain of events spaced exactly at the 30-minute
        // timeout (a 1800s gap is NOT > 1800s, so it never splits): a chain
        // spanning exactly 4h stays, one spanning longer is dropped whole —
        // the SQL's HAVING duration <= max.
        let chain = |sid: &str, span_secs: i64| {
            let start = chrono::DateTime::parse_from_rfc3339("2026-09-14T10:00:00Z")
                .unwrap()
                .with_timezone(&chrono::Utc);
            let steps = (span_secs / 1800) + 1;
            (0..steps)
                .map(|i| {
                    let ts = (start + chrono::Duration::seconds(i * 1800)).to_rfc3339();
                    get_line(&ts, &format!("type=pageview&sid={sid}"))
                })
                .collect::<Vec<_>>()
        };

        let day = NaiveDate::from_ymd_opt(2026, 9, 14).unwrap();
        let records = records_from_lines(
            &chain("at-cap", 4 * 3600)
                .into_iter()
                .chain(chain("over-cap", 4 * 3600 + 1800))
                .collect::<Vec<_>>(),
        );

        let rows = sessionize_day(day, &records, &SessionConfig::default());
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].session_id, "at-cap_20260914_1");
        assert_eq!(rows[0].duration_seconds, 4 * 3600);
    }

    #[test]
    fn sessionize_day_ignores_events_without_session_id() {
        let records = records_from_lines(&[
            get_line("2026-09-14T10:00:00Z", "type=pageview"),
            get_line("2026-09-14T10:01:00Z", "type=pageview&sid=s9"),
        ]);
        let rows = sessionize_day(
            NaiveDate::from_ymd_opt(2026, 9, 14).unwrap(),
            &records,
            &SessionConfig::default(),
        );
        assert_eq!(rows.len(), 1);
        assert!(rows[0].session_id.starts_with("s9_"));
    }

    #[test]
    fn sessions_parquet_schema_matches_ddl_order() {
        use arrow::array::Array;

        let row = SessionRow {
            session_id: "s1_20260914_1".to_string(),
            user_id: Some("u1".to_string()),
            started_at: chrono::Utc::now(),
            ended_at: chrono::Utc::now(),
            pageviews: 1,
            clicks: 0,
            scrolls: 0,
            dwells: 0,
            event_count: 1,
            entry_url: "/p".to_string(),
            exit_url: "/p".to_string(),
            network: Some("taboola".to_string()),
            campaign_id: Some("c1".to_string()),
            campaign_name: None,
            creative_id: Some("cr1".to_string()),
            headline: Some("H".to_string()),
            converted: false,
            conversion_value: 0.0,
            device_type: None,
            device_os: None,
            referrer: None,
            duration_seconds: 0,
            bounce: true,
            depth: 1,
        };

        let bytes = sessions_to_parquet(&[row]).unwrap();
        let batches = read_parquet_batches(&bytes);
        let schema = batches[0].schema();

        let expected: &[(&str, &str)] = &[
            ("session_id", "Utf8"),
            ("user_id", "Utf8"),
            ("started_at", "Timestamp(Microsecond, None)"),
            ("ended_at", "Timestamp(Microsecond, None)"),
            ("pageviews", "Int32"),
            ("clicks", "Int32"),
            ("scrolls", "Int32"),
            ("dwells", "Int32"),
            ("event_count", "Int32"),
            ("entry_url", "Utf8"),
            ("exit_url", "Utf8"),
            ("network", "Utf8"),
            ("campaign_id", "Utf8"),
            ("campaign_name", "Utf8"),
            ("creative_id", "Utf8"),
            ("headline", "Utf8"),
            ("converted", "Boolean"),
            ("conversion_value", "Float64"),
            ("device_type", "Utf8"),
            ("device_os", "Utf8"),
            ("referrer", "Utf8"),
            ("duration_seconds", "Int32"),
            ("bounce", "Boolean"),
            ("depth", "Int32"),
        ];

        assert_eq!(schema.fields().len(), expected.len());
        for (i, (name, type_str)) in expected.iter().enumerate() {
            assert_eq!(schema.field(i).name(), *name, "column {i}");
            assert_eq!(
                &format!("{:?}", schema.field(i).data_type()),
                type_str,
                "column {name} type"
            );
        }
        assert_eq!(batches[0].num_rows(), 1);

        // Column presence sanity via array access (silences unused-import
        // machinery in one place and proves the arrays materialize)
        assert!(batches[0].column(0).len() == 1);
    }

    // ------------------------------------------------------------------
    // Checkpoint
    // ------------------------------------------------------------------

    #[test]
    fn checkpoint_roundtrip_and_atomic_save() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("replay-state").join("checkpoint.json");

        let mut cp = Checkpoint::default();
        assert!(!cp.is_file_complete("raw-20260914-05"));
        cp.record_file(
            "raw-20260914-05",
            CompletedFile {
                keys: vec!["trace-events/events/pageview/date=2026-09-14/hour=05/replay-raw-20260914-05.parquet".into()],
                rows: 7,
                parse_errors: 1,
                completed_at: chrono::Utc::now(),
            },
        );
        cp.save(&path).unwrap();

        // No temp file survives a successful save
        assert!(!path.with_extension("json.tmp").exists());

        let loaded = Checkpoint::load(&path);
        assert_eq!(loaded, cp);
        assert!(loaded.is_file_complete("raw-20260914-05"));

        // Sessions coverage: superset counts, subset does not
        let day = NaiveDate::from_ymd_opt(2026, 9, 14).unwrap();
        assert!(!loaded.sessions_covered(day, &["raw-20260914-05".to_string()]));
        let mut cp2 = loaded;
        cp2.record_sessions(
            day,
            CompletedSessions {
                stems: vec!["raw-20260914-05".into(), "raw-20260914-06".into()],
                rows: 3,
                completed_at: chrono::Utc::now(),
            },
        );
        assert!(cp2.sessions_covered(day, &["raw-20260914-05".to_string()]));
        assert!(cp2.sessions_covered(day, &[]));
        assert!(!cp2.sessions_covered(day, &["raw-20260914-07".to_string()]));

        // A corrupt checkpoint starts fresh rather than failing the run —
        // deterministic keys make re-upload a safe recovery
        fs::write(&path, "{ not json").unwrap();
        assert_eq!(Checkpoint::load(&path), Checkpoint::default());
    }

    // ------------------------------------------------------------------
    // Orchestration
    // ------------------------------------------------------------------

    fn two_files_with_events(dir: &Path) {
        // 04:40 -> 05:00 is a 20-minute gap: under the 30-minute timeout,
        // so the sessions stage sees one continuous s1 session across the
        // two hour files
        write_raw(
            dir,
            "raw-20260914-04",
            &[get_line(
                "2026-09-14T04:40:00Z",
                "type=pageview&sid=s1&utm_source=taboola&utm_campaign=c1&tb_headline=H1",
            )],
        );
        write_raw(
            dir,
            "raw-20260914-05",
            &[
                get_line(
                    "2026-09-14T05:00:00Z",
                    "type=pageview&sid=s1&utm_source=taboola&utm_campaign=c1&tb_headline=H1",
                ),
                get_line("2026-09-14T05:10:00Z", "type=click&sid=s1"),
            ],
        );
    }

    #[tokio::test]
    async fn run_replay_uploads_and_checkpoints() {
        let dir = tempfile::tempdir().unwrap();
        two_files_with_events(dir.path());
        let cp_path = dir.path().join(".replay").join("checkpoint.json");

        let cfg = test_config(dir.path(), &cp_path);
        let sink = MemorySink::new();
        let outcome = run_replay(&cfg, &sink).await.unwrap();

        assert!(outcome.ok());
        assert_eq!(outcome.files_selected, 2);
        assert_eq!(outcome.files_processed, 2);
        assert_eq!(outcome.events_rows, 3);
        let mut expected_keys = vec![
            events_object_key("trace-events", "pageview", &hour("2026-09-14", 4)),
            events_object_key("trace-events", "pageview", &hour("2026-09-14", 5)),
            events_object_key("trace-events", "click", &hour("2026-09-14", 5)),
        ];
        expected_keys.sort();
        let mut got = sink.keys();
        got.sort();
        assert_eq!(got, expected_keys);
        assert_eq!(outcome.output_keys.len(), 3);

        // Both files recorded; skip-sessions is set so no day outcomes
        let cp = Checkpoint::load(&cp_path);
        assert!(cp.is_file_complete("raw-20260914-04"));
        assert!(cp.is_file_complete("raw-20260914-05"));
        assert!(outcome.sessions.is_empty());
    }

    #[tokio::test]
    async fn run_replay_rerun_is_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        two_files_with_events(dir.path());
        let cp_path = dir.path().join(".replay").join("checkpoint.json");
        let cfg = test_config(dir.path(), &cp_path);

        let sink = MemorySink::new();
        let first = run_replay(&cfg, &sink).await.unwrap();
        assert!(first.ok());

        // Second run over the same range: nothing new uploaded
        let sink2 = MemorySink::new();
        let second = run_replay(&cfg, &sink2).await.unwrap();
        assert!(second.ok());
        assert_eq!(second.files_selected, 2);
        assert_eq!(second.files_skipped_checkpoint, 2);
        assert_eq!(second.files_processed, 0);
        assert!(sink2.keys().is_empty(), "checkpoint must prevent re-upload");

        // --force reprocesses: same keys, uploaded again (overwrite)
        let mut forced = cfg.clone();
        forced.force = true;
        let sink3 = MemorySink::new();
        let third = run_replay(&forced, &sink3).await.unwrap();
        assert!(third.ok());
        assert_eq!(third.files_processed, 2);
        let mut a = sink.keys();
        let mut b = sink3.keys();
        a.sort();
        b.sort();
        assert_eq!(a, b, "force must overwrite the same keys, not add new ones");
    }

    #[tokio::test]
    async fn run_replay_resumes_after_upload_failure() {
        let dir = tempfile::tempdir().unwrap();
        two_files_with_events(dir.path());
        let cp_path = dir.path().join(".replay").join("checkpoint.json");
        let cfg = test_config(dir.path(), &cp_path);

        // First put of the first file fails: no file completes, but the run
        // finishes (failure is counted, not fatal)
        let failing = MemorySink::with_failures(1);
        let first = run_replay(&cfg, &failing).await.unwrap();
        assert!(!first.ok());
        assert_eq!(first.upload_failures, 1);
        assert_eq!(first.files_processed, 1, "later files still ran");
        let cp = Checkpoint::load(&cp_path);
        assert!(!cp.is_file_complete("raw-20260914-04"));
        assert!(cp.is_file_complete("raw-20260914-05"));

        // Resume: only the failed file is redone
        let sink = MemorySink::new();
        let second = run_replay(&cfg, &sink).await.unwrap();
        assert!(second.ok());
        assert_eq!(second.files_skipped_checkpoint, 1);
        assert_eq!(second.files_processed, 1);
        assert_eq!(
            sink.keys(),
            vec![events_object_key(
                "trace-events",
                "pageview",
                &hour("2026-09-14", 4)
            )]
        );
    }

    #[tokio::test]
    async fn run_replay_dry_run_touches_nothing() {
        let dir = tempfile::tempdir().unwrap();
        two_files_with_events(dir.path());
        let cp_path = dir.path().join(".replay").join("checkpoint.json");
        let mut cfg = test_config(dir.path(), &cp_path);
        cfg.dry_run = true;
        // Sessions stage is a no-op on dry runs even when not skipped
        cfg.skip_sessions = false;

        let sink = MemorySink::new();
        let outcome = run_replay(&cfg, &sink).await.unwrap();
        assert!(outcome.ok());
        assert!(sink.keys().is_empty(), "dry run must not upload");
        assert!(!cp_path.exists(), "dry run must not write a checkpoint");
        assert_eq!(outcome.output_keys.len(), 3, "planned keys are reported");
    }

    #[tokio::test]
    async fn sessions_stage_guards_partial_days() {
        let dir = tempfile::tempdir().unwrap();
        // Only 2 of 24 hours for the day
        two_files_with_events(dir.path());
        let cp_path = dir.path().join(".replay").join("checkpoint.json");
        let mut cfg = test_config(dir.path(), &cp_path);
        cfg.skip_sessions = false;

        let sink = MemorySink::new();
        let outcome = run_replay(&cfg, &sink).await.unwrap();
        assert_eq!(outcome.sessions.len(), 1);
        assert_eq!(
            outcome.sessions[0].status,
            DayStatus::SkippedPartial { hours_present: 2 }
        );
        assert!(!sink.keys().iter().any(|k| k.contains("iceberg/sessions")));

        // allow-partial-days writes the day's sessions from those hours
        let mut cfg = cfg.clone();
        cfg.allow_partial_days = true;
        cfg.force = true; // events are already checkpointed
        let sink = MemorySink::new();
        let outcome = run_replay(&cfg, &sink).await.unwrap();
        assert!(outcome.ok());
        assert_eq!(outcome.sessions[0].status, DayStatus::Written);
        assert_eq!(outcome.sessions[0].rows, 1, "one continuous s1 session");
        assert!(sink.keys().contains(&sessions_object_key(
            "trace-events",
            NaiveDate::from_ymd_opt(2026, 9, 14).unwrap()
        )));

        // A re-run without force skips the day: the checkpoint's stems cover
        // the selection (same two stems)
        let mut cfg = cfg.clone();
        cfg.force = false;
        cfg.allow_partial_days = true;
        let sink = MemorySink::new();
        let outcome = run_replay(&cfg, &sink).await.unwrap();
        assert_eq!(outcome.sessions[0].status, DayStatus::SkippedCheckpoint);
        assert!(!sink.keys().iter().any(|k| k.contains("iceberg/sessions")));
    }

    /// A day with all 24 hour files sessionizes; a day with NO files in the
    /// selection is guarded off like any partial day — writing an empty
    /// sessions file from zero evidence would erase the nightly
    /// materialization for raw hours the replay never saw.
    #[tokio::test]
    async fn sessions_stage_full_day_written_missing_day_guarded() {
        let dir = tempfile::tempdir().unwrap();
        // 24 hour files: 05 has the traffic, the rest are empty valid files
        for h in 0..24 {
            if h == 5 {
                write_raw(
                    dir.path(),
                    &format!("raw-20260914-{h:02}"),
                    &[
                        get_line(
                            "2026-09-14T05:00:00Z",
                            "type=pageview&sid=s1&utm_source=taboola&utm_campaign=c1",
                        ),
                        get_line("2026-09-14T05:10:00Z", "type=click&sid=s1"),
                    ],
                );
            } else {
                // An hour with no accepted requests can still exist as an
                // (empty) file in the archive
                std::fs::write(dir.path().join(format!("raw-20260914-{h:02}.jsonl")), "").unwrap();
            }
        }
        // And a second day with NO files at all, inside the range
        let cp_path = dir.path().join(".replay").join("checkpoint.json");
        let mut cfg = test_config(dir.path(), &cp_path);
        cfg.skip_sessions = false;
        cfg.to = HourKey::parse_bound("20260915", true).unwrap();

        let sink = MemorySink::new();
        let outcome = run_replay(&cfg, &sink).await.unwrap();
        assert!(outcome.ok());
        assert_eq!(outcome.sessions.len(), 2);
        assert_eq!(outcome.sessions[0].status, DayStatus::Written);
        assert_eq!(outcome.sessions[0].rows, 1);
        assert_eq!(
            outcome.sessions[1].status,
            DayStatus::SkippedPartial { hours_present: 0 },
            "a day with no selected files must not be overwritten from zero evidence"
        );
        assert!(!sink.keys().contains(&sessions_object_key(
            "trace-events",
            NaiveDate::from_ymd_opt(2026, 9, 15).unwrap()
        )));

        // Only the materialized day is recorded
        let cp = Checkpoint::load(&cp_path);
        assert_eq!(cp.sessions.len(), 1);
    }
}
