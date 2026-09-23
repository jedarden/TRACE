# Raw-Log Replay and Backfill

How to reprocess archived raw logs through the pipeline with the
`trace-replay` CLI (`flusher/src/bin/trace-replay.rs`, library in
`flusher/src/replay.rs`). Raw logs are the documented source of truth
("log first, parse later" — `docs/notes/collector-api.md`,
`docs/plan/plan.md`); replay is that promise made executable: when parsing,
normalization, or sessionization improves, reprocess the affected hours and
the derived data is rebuilt from the raw lines. Replay reruns overwrite their
own deterministic objects; replay and live outputs can coexist.

The live flusher is one-pass (parse → Parquet → S3 → **delete**) and cannot
do this. Replay reads the archived raw files without modifying or deleting
them.

---

## Usage

```bash
# Reprocess one week, events + sessions (env config as below)
trace-replay /data/raw-archive --from 20260901-00 --to 20260907-23

# One day, events only
trace-replay --from 20260914 --to 20260914 --skip-sessions

# See what a range would produce, upload nothing
trace-replay --from 20260914 --to 20260914 --dry-run

# Redo a range the checkpoint already recorded (e.g. after a mapping change)
trace-replay --from 20260914 --to 20260914 --force
```

Environment (same variables and defaults as the flusher daemon):
`TRACE_S3_BUCKET` (required), `TRACE_S3_REGION` (`us-east-1`),
`TRACE_S3_PREFIX` (`trace-events`), `TRACE_S3_ENDPOINT` (optional, MinIO et
al). Without a positional `RAW_DIR`, replay reads `$TRACE_LOG_DIR` or
`/data/logs`.

Range bounds are `YYYYMMDD-HH` or `YYYYMMDD` (whole day: hours 00–23),
inclusive on both ends. Raw files are recognized in the collector's three
forms — `raw-YYYYMMDD-HH.jsonl`, `raw-YYYYMMDD-HH.jsonl.ready` (rotated,
sealed), and `raw-YYYYMMDD-HH.jsonl.gz` (archive). If the same hour exists
in more than one form, the `.ready` file wins over the still-open `.jsonl`,
which wins over the `.gz`.

Exit status is `1` if any upload failed; the run still completes what it
can, and re-running the same command resumes from the checkpoint.

## What each stage does

1. **Parse** — the same `RawLogParser` as the live path, so a replay is
   what a fresh flush would have produced from the same lines. Unparseable
   lines are counted (in the summary and the checkpoint) and skipped; they
   remain in the raw log for a future replay.
2. **Impression dedup** — within one raw file, repeated impression sends
   carrying the same `imp_id` collapse to the first arrival. Other event types
   are retained even when they carry the same `imp_id`.
3. **Normalize + extract assets** — the config-driven mapping
   (`flusher/src/network_mapping.toml`) detects the ad network and maps
   network-specific params onto the canonical columns `network`,
   `campaign_id`, `creative_id`, `headline`, `image_id`, `item_id`. Raw
   params are never rewritten; they stay in the `params` map verbatim. The
   embedded mapping is the default; `--mapping <file>` overrides it.
4. **Sessionize** — see below.
5. **Upload** — one Parquet object per (hour, event type), plus one
   sessions object per day.

## Output layout and idempotency

```
<prefix>/events/<type>/date=YYYY-MM-DD/hour=HH/replay-raw-YYYYMMDD-HH.parquet
<prefix>/iceberg/sessions/data/started_at_day=YYYY-MM-DD/sessions-YYYY-MM-DD.parquet
```

Every key is a pure function of its inputs (no UUIDs, no timestamps), so a
re-run **overwrites the same replay objects** instead of stacking another
replay copy beside them. Replay writes below `<prefix>/events/`; the current
live flusher writes below `<prefix>/<type>/`, so the two roots must not be
treated as replacements until the analytics source paths are unified. The
event schema is the EV3 column set; enrichment-owned columns
(`quality_score`, `is_valid`, attribution, device, ...) are present but NULL.

Replay event files are named `replay-raw-*`, while live-flusher files use
UUID names. A live file and a replay file for the same hour therefore remain
separate objects; replay is idempotent against itself, not against an earlier
live flush.

## Sessionization

`sessionize_day` in `flusher/src/replay.rs` is a faithful port of the
authoritative DuckDB query in `analytics/src/session_materializer.rs`
(`sessionization_query`): UTC-day window, events with a `session_id` only,
split on gaps strictly greater than 30 minutes, `HAVING` duration ≤ 4h,
day-qualified session ids (`<sid>_<YYYYMMDD>_<seq>`), first-touch
attribution (earliest non-NULL value, like `arg_min`), bounce =
single-event session, depth = distinct URLs. Its output goes to the exact
key the nightly materializer writes (`sessions-<day>.parquet` under
`started_at_day=<day>/`), so a replayed day **replaces** that day's
materialization.

**The two implementations must stay in sync.** The fixture tests in
`replay.rs` mirror the materializer's end-to-end fixtures; if you change
one side, change both and re-run both test suites.

Known shared quirk, kept deliberately: the materializer's SQL counts the
literal `type = 'dwell'`, a string the parser never emits (dwell heartbeats
are `heartbeat`). Replay matches the SQL rather than "fixing" it so the two
writers of `sessions-<day>.parquet` cannot disagree.

### The partial-day guard

Sessions are recomputed only from the raw files in the current selection.
Overwriting `sessions-<day>.parquet` from an incomplete day would silently
drop sessions from the missing hours, so a day is sessionized only when all
24 of its hour files are selected. `--allow-partial-days` overrides — the
day's sessions then reflect only the selected hours, which is correct when
you are deliberately rebuilding from a partial archive. Days with **no**
selected files are always guarded off: writing an empty sessions file from
zero evidence would erase the nightly materialization for hours the replay
never saw.

## Checkpointing

The checkpoint is one JSON file, `<raw-dir>/.replay/checkpoint.json` by
default (`--checkpoint-dir` overrides), rewritten atomically
(temp + rename) after every completed unit:

- `completed` — per raw-file stem (`raw-YYYYMMDD-HH`): the output keys that
  landed, row/parse-error counts. A file present here is skipped on the
  next run unless `--force`.
- `sessions` — per day: the stems the sessionization consumed. A day is
  skipped when the recorded stems are a **superset** of the current
  selection's stems for that day (a wider later selection recomputes).

A corrupt checkpoint is warned about and treated as fresh — safe because
keys are deterministic, so redoing a unit overwrites rather than duplicates.
An interrupted run (upload failure, crash) simply leaves the unfinished
units unrecorded; re-run the same command to resume. A file is recorded
only after **all** of its per-type uploads succeeded, so a half-uploaded
hour is retried whole.

`--dry-run` parses and reports the planned keys but uploads nothing and
writes no checkpoint.

## Coexistence with the live flusher

Replay is read-only over the raw directory and never deletes inputs, so it
can run alongside the daemon. Mind the shared `.replay` checkpoint when two
replays run over the same raw dir concurrently — the checkpoint has no
locking; serialize replays over one directory (or give each a
`--checkpoint-dir`).
