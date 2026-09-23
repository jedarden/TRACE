//! trace-flusher library target.
//!
//! The flusher binary (`src/main.rs`) is the live daemon: it watches the
//! collector's log directory and processes each raw log file exactly once,
//! deleting it after upload. The library holds the pieces that outlive that
//! one-pass design:
//!
//! - [`raw_log_parser`] — raw collector log line → structured event
//! - [`normalizer`] — config-driven cross-network parameter normalization
//! - [`replay`] — raw-log replay/backfill over a selected hour range
//!
//! Raw logs are the documented source of truth ("log first, parse later" —
//! if ETL logic improves, replay from the beginning), and [`replay`] is that
//! promise made executable: it reprocesses archived `raw-YYYYMMDD-HH` files
//! through parsing, normalization, sessionization, and asset extraction with
//! checkpointing and idempotent output handling. The `trace-replay` binary
//! (`src/bin/trace-replay.rs`) is its CLI.
//!
//! The daemon's own modules (S3 batching, file watching, the DLQ) stay in
//! `src/main.rs`; they are inherently single-pass and have no replay use.

// `raw_log_parser` and `normalizer` carry inherent `from_str` constructors
// whose naming predates this library target and is pinned by use throughout
// the daemon. As `pub` items of a lib they become externally reachable and
// trip `clippy::should_implement_trait`, which the bin target never saw
// because its modules are crate-private.
#![allow(clippy::should_implement_trait)]

pub mod normalizer;
pub mod raw_log_parser;
pub mod replay;

use anyhow::Result;
use arrow::array::{
    BooleanArray, Float64Array, Int64Array, MapArray, StringArray, TimestampMillisecondArray,
};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::{arrow::arrow_writer::ArrowWriter, file::properties::WriterProperties};
use std::sync::Arc;

pub fn parsed_events_to_parquet(events: Vec<raw_log_parser::Event>) -> Result<Vec<u8>> {
    let n = events.len();
    let timestamps: Vec<i64> = events.iter().map(|e| e.ts.timestamp_millis()).collect();
    let ips: Vec<Option<String>> = events.iter().map(|e| e.ip.clone()).collect();
    let uas: Vec<Option<String>> = events.iter().map(|e| e.ua.clone()).collect();
    let urls: Vec<String> = events.iter().map(|e| e.url.clone()).collect();
    let types: Vec<String> = events
        .iter()
        .map(|e| e.event_type.as_str().to_string())
        .collect();
    let session_ids: Vec<Option<String>> = events.iter().map(|e| e.session_id.clone()).collect();
    let user_ids: Vec<Option<String>> = events.iter().map(|e| e.user_id.clone()).collect();
    let cookie_ids: Vec<Option<String>> = events.iter().map(|e| e.cookie_id.clone()).collect();
    let referrers: Vec<Option<String>> = events.iter().map(|e| e.referer.clone()).collect();
    let referrer_networks: Vec<Option<String>> =
        events.iter().map(|e| e.referrer_network.clone()).collect();
    let networks: Vec<Option<String>> = vec![None; n];
    let campaign_ids: Vec<Option<String>> = vec![None; n];
    let campaign_names: Vec<Option<String>> = vec![None; n];
    let creative_ids: Vec<Option<String>> = vec![None; n];
    let headlines: Vec<Option<String>> = vec![None; n];
    let image_ids: Vec<Option<String>> = vec![None; n];
    let item_ids: Vec<Option<String>> = vec![None; n];
    let attribution_campaign_ids: Vec<Option<String>> = vec![None; n];
    let attribution_creative_ids: Vec<Option<String>> = vec![None; n];
    let attribution_touches: Vec<Option<i64>> = vec![None; n];
    let attribution_days_to_convert: Vec<Option<i64>> = vec![None; n];
    let device_types: Vec<Option<String>> = vec![None; n];
    let device_oss: Vec<Option<String>> = vec![None; n];
    let device_browsers: Vec<Option<String>> = vec![None; n];
    let scroll_depth_pcts: Vec<Option<i64>> = events
        .iter()
        .map(|event| {
            if event.event_type != raw_log_parser::EventType::Scroll {
                return None;
            }
            event
                .params
                .get("scroll_depth")
                .and_then(|value| value.parse::<i64>().ok())
                .filter(|value| (0..=100).contains(value))
        })
        .collect();
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
    let mut all_params_keys = Vec::new();
    let mut all_params_values = Vec::new();
    let mut params_offsets = vec![0i32];
    let mut current_offset = 0i32;
    for event in &events {
        for (key, value) in &event.params {
            all_params_keys.push(key.clone());
            all_params_values.push(value.clone());
            current_offset += 1;
        }
        params_offsets.push(current_offset);
    }
    let map_fields = vec![
        Field::new("key", DataType::Utf8, false),
        Field::new("value", DataType::Utf8, false),
    ];
    let map_data_type = DataType::Map(
        Arc::new(Field::new(
            "entries",
            DataType::Struct(map_fields.into()),
            false,
        )),
        false,
    );
    let params_array = MapArray::new(
        Arc::new(Field::new(
            "entries",
            DataType::Struct(
                vec![
                    Field::new("key", DataType::Utf8, false),
                    Field::new("value", DataType::Utf8, false),
                ]
                .into(),
            ),
            false,
        )),
        OffsetBuffer::new(params_offsets.into()),
        arrow::array::StructArray::new(
            vec![
                Field::new("key", DataType::Utf8, false),
                Field::new("value", DataType::Utf8, false),
            ]
            .into(),
            vec![
                Arc::new(StringArray::from(all_params_keys)),
                Arc::new(StringArray::from(all_params_values)),
            ],
            None,
        ),
        None,
        false,
    );
    let schema = Schema::new(vec![
        Field::new(
            "ts",
            DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
            false,
        ),
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
        Field::new(
            "enriched_at",
            DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
            true,
        ),
        Field::new("enrichment_version", DataType::Utf8, true),
        Field::new("params", map_data_type, true),
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
    let properties = WriterProperties::builder().build();
    let mut writer = ArrowWriter::try_new(&mut buffer, batch.schema(), Some(properties))?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(buffer)
}
