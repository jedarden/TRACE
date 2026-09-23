//! Iceberg schema / partition-spec model with spec-exact JSON serialization,
//! plus derivation of a table schema from the Parquet files producers write.
//!
//! Schema JSON shapes (spec §"Table Metadata" / §"Schemas"):
//!   "schemas": [ { "type": "struct", "schema-id": 0, "fields": [
//!       { "id": 1, "name": "ts", "required": true, "type": "timestamp" }, ... ],
//!       "identifier-field-ids": [1] } ]
//!
//! Deriving the schema from the producer's actual Parquet footer (rather
//! than hand-maintaining a second copy) is what keeps table metadata and
//! data files in lockstep — a drifted hand-written schema is unreadable by
//! engines that type-check data files against it.

use anyhow::{bail, Context, Result};
use bytes::Bytes;
use chrono::Datelike;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::spec;

/// Primitive + nested Iceberg types TRACE tables use.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IcebergType {
    Boolean,
    Int,
    Long,
    Float,
    Double,
    String,
    Timestamp,
    Timestamptz,
    Date,
    Map(Box<IcebergType>, Box<IcebergType>),
    List(Box<IcebergType>),
}

impl IcebergType {
    pub fn spec_name(&self) -> String {
        match self {
            IcebergType::Boolean => "boolean".into(),
            IcebergType::Int => "int".into(),
            IcebergType::Long => "long".into(),
            IcebergType::Float => "float".into(),
            IcebergType::Double => "double".into(),
            IcebergType::String => "string".into(),
            IcebergType::Timestamp => "timestamp".into(),
            IcebergType::Timestamptz => "timestamptz".into(),
            IcebergType::Date => "date".into(),
            IcebergType::Map(k, v) => format!("map<{}, {}>", k.spec_name(), v.spec_name()),
            IcebergType::List(v) => format!("list<{}>", v.spec_name()),
        }
    }

    fn from_spec_name(s: &str) -> Result<Self> {
        let s = s.trim();
        Ok(match s {
            "boolean" => IcebergType::Boolean,
            "int" => IcebergType::Int,
            "long" => IcebergType::Long,
            "float" => IcebergType::Float,
            "double" => IcebergType::Double,
            "string" => IcebergType::String,
            "timestamp" => IcebergType::Timestamp,
            "timestamptz" => IcebergType::Timestamptz,
            "date" => IcebergType::Date,
            _ => {
                if let Some(inner) = s.strip_prefix("map<").and_then(|x| x.strip_suffix('>')) {
                    // Split at the top-level ", " only (values may be nested).
                    let (k, v) = split_top_level(inner)
                        .context("malformed map type - missing key/value split")?;
                    return Ok(IcebergType::Map(
                        Box::new(IcebergType::from_spec_name(k)?),
                        Box::new(IcebergType::from_spec_name(v)?),
                    ));
                }
                if let Some(inner) = s.strip_prefix("list<").and_then(|x| x.strip_suffix('>')) {
                    return Ok(IcebergType::List(Box::new(IcebergType::from_spec_name(
                        inner,
                    )?)));
                }
                bail!("unsupported Iceberg type: {s}")
            }
        })
    }
}

fn split_top_level(s: &str) -> Option<(&str, &str)> {
    let mut depth = 0i32;
    for (i, c) in s.char_indices() {
        match c {
            '<' => depth += 1,
            '>' => depth -= 1,
            ',' if depth == 0 => {
                let (a, b) = (&s[..i], &s[i + 1..]);
                return Some((a.trim(), b.trim()));
            }
            _ => {}
        }
    }
    None
}

impl Serialize for IcebergType {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.spec_name())
    }
}

impl<'de> Deserialize<'de> for IcebergType {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        IcebergType::from_spec_name(&s).map_err(serde::de::Error::custom)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Field {
    pub id: i32,
    pub name: String,
    pub required: bool,
    #[serde(rename = "type")]
    pub field_type: IcebergType,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub doc: Option<String>,
}

/// A table schema: `{"type":"struct","schema-id":0,"fields":[...], ...}`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Schema {
    #[serde(rename = "type")]
    struct_type: String,
    #[serde(rename = "schema-id")]
    pub schema_id: i32,
    pub fields: Vec<Field>,
    #[serde(
        rename = "identifier-field-ids",
        skip_serializing_if = "Option::is_none"
    )]
    pub identifier_field_ids: Option<Vec<i32>>,
}

impl Schema {
    pub fn new(fields: Vec<Field>, identifier_field_ids: Option<Vec<i32>>) -> Self {
        Self {
            struct_type: "struct".to_string(),
            schema_id: 0,
            fields,
            identifier_field_ids,
        }
    }

    pub fn field(&self, id: i32) -> Option<&Field> {
        self.fields.iter().find(|f| f.id == id)
    }

    pub fn highest_field_id(&self) -> i32 {
        self.fields.iter().map(|f| f.id).max().unwrap_or(0)
    }

    /// Structural comparison used when a producer commits into an existing
    /// table: every declared field must exist with the same name/type/required.
    pub fn is_compatible_with(&self, other: &Schema) -> bool {
        self.fields.len() == other.fields.len()
            && self.fields.iter().zip(other.fields.iter()).all(|(a, b)| {
                a.id == b.id
                    && a.name == b.name
                    && a.required == b.required
                    && a.field_type == b.field_type
            })
    }
}

/// Partition transforms TRACE writes: `identity` on string fields and
/// `day` on timestamps. Result types per spec §"Partition transforms":
/// identity keeps the source type, day yields int (days from epoch).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Transform {
    Identity,
    Day,
}

impl Transform {
    pub fn spec_name(&self) -> &'static str {
        match self {
            Transform::Identity => "identity",
            Transform::Day => "day",
        }
    }

    pub fn result_type(&self, source: &IcebergType) -> Result<IcebergType> {
        match self {
            Transform::Identity => Ok(source.clone()),
            Transform::Day => match source {
                IcebergType::Timestamp | IcebergType::Timestamptz | IcebergType::Date => {
                    Ok(IcebergType::Int)
                }
                other => bail!(
                    "day transform requires a date/timestamp source, got {}",
                    other.spec_name()
                ),
            },
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartitionField {
    #[serde(rename = "source-id")]
    pub source_id: i32,
    #[serde(rename = "field-id")]
    pub field_id: i32,
    pub name: String,
    pub transform: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartitionSpec {
    #[serde(rename = "spec-id")]
    pub spec_id: i32,
    pub fields: Vec<PartitionField>,
}

impl PartitionSpec {
    pub fn new(fields: Vec<PartitionField>) -> Self {
        Self { spec_id: 0, fields }
    }

    /// Typed accessor used when building manifests: resolve each field's
    /// transform result type against the table schema.
    pub fn result_types(&self, schema: &Schema) -> Result<Vec<(PartitionField, IcebergType)>> {
        self.fields
            .iter()
            .map(|f| {
                let source = schema.field(f.source_id).with_context(|| {
                    format!(
                        "partition field {} names unknown source id {}",
                        f.name, f.source_id
                    )
                })?;
                let transform = if f.transform == "day" {
                    Transform::Day
                } else if f.transform == "identity" {
                    Transform::Identity
                } else {
                    bail!("unsupported partition transform {}", f.transform)
                };
                Ok((f.clone(), transform.result_type(&source.field_type)?))
            })
            .collect()
    }
}

/// A typed partition value as recorded in manifests. `day` transforms
/// carry days-since-epoch ints; identity carries the source type.
#[derive(Debug, Clone, PartialEq)]
pub enum PartitionValue {
    Int(i64),
    Long(i64),
    String(String),
    Boolean(bool),
}

impl PartitionValue {
    pub fn spec_name(&self) -> &'static str {
        match self {
            PartitionValue::Int(_) => "int",
            PartitionValue::Long(_) => "long",
            PartitionValue::String(_) => "string",
            PartitionValue::Boolean(_) => "boolean",
        }
    }
}

/// Days since 1970-01-01 for a UTC date — the value a `day(ts)` partition
/// transform produces. Producers compute this from the event timestamp.
pub fn days_since_epoch(date: chrono::NaiveDate) -> i64 {
    i64::from(
        date.num_days_from_ce()
            - chrono::NaiveDate::from_ymd_opt(1970, 1, 1)
                .unwrap()
                .num_days_from_ce(),
    )
}

/// Derive the table schema from a Parquet file's Arrow schema + footer.
///
/// Returns (schema, row_count). Field ids are assigned in column order
/// starting at 1, matching how the flusher/materializer write columns.
pub fn schema_from_parquet(parquet_bytes: &[u8]) -> Result<(Schema, i64)> {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    let builder = ParquetRecordBatchReaderBuilder::try_new(Bytes::copy_from_slice(parquet_bytes))
        .context("failed to open Parquet footer")?;
    let num_rows = builder.metadata().file_metadata().num_rows();
    let arrow_schema = builder.schema().clone();

    let mut fields = Vec::with_capacity(arrow_schema.fields().len());
    for (i, f) in arrow_schema.fields().iter().enumerate() {
        let field_type = arrow_to_iceberg(f.data_type())
            .with_context(|| format!("column {} ({})", i, f.name()))?;
        fields.push(Field {
            id: (i + 1) as i32,
            name: f.name().clone(),
            required: !f.is_nullable(),
            field_type,
            doc: None,
        });
    }

    Ok((Schema::new(fields, None), num_rows))
}

fn arrow_to_iceberg(dt: &arrow::datatypes::DataType) -> Result<IcebergType> {
    use arrow::datatypes::DataType as A;
    Ok(match dt {
        A::Boolean => IcebergType::Boolean,
        A::Int8 | A::Int16 | A::Int32 | A::UInt8 | A::UInt16 | A::UInt32 => IcebergType::Int,
        A::Int64 | A::UInt64 => IcebergType::Long,
        A::Float16 | A::Float32 => IcebergType::Float,
        A::Float64 => IcebergType::Double,
        A::Utf8 | A::LargeUtf8 | A::Binary | A::LargeBinary => IcebergType::String,
        A::Date32 => IcebergType::Date,
        A::Timestamp(unit, tz) => match tz {
            None => IcebergType::Timestamp,
            Some(_) => IcebergType::Timestamptz,
        }
        .tap_adjust_unit(*unit),
        A::Map(_, _) | A::Struct(_) => {
            // Flusher writes params as Map(key=Utf8, value=Utf8); materialized
            // tables use the same shape. Extract key/value types for maps.
            match dt {
                A::Map(field, _) => {
                    let entries = match field.data_type() {
                        A::Struct(fields) if fields.len() == 2 => {
                            (fields[0].clone(), fields[1].clone())
                        }
                        other => bail!("unsupported map entry shape {other:?}"),
                    };
                    IcebergType::Map(
                        Box::new(arrow_to_iceberg(entries.0.data_type())?),
                        Box::new(arrow_to_iceberg(entries.1.data_type())?),
                    )
                }
                A::Struct(_) => {
                    bail!("struct columns are not representable in TRACE Iceberg tables")
                }
                _ => unreachable!(),
            }
        }
        A::List(item) | A::LargeList(item) => {
            IcebergType::List(Box::new(arrow_to_iceberg(item.data_type())?))
        }
        other => bail!("unsupported Arrow type for Iceberg schema derivation: {other:?}"),
    })
}

/// Timestamp precision does not change the Iceberg type (both milli and
/// micro map to `timestamp`/`timestamptz`); this helper exists to keep the
/// match above exhaustive without dead logic.
trait TapAdjustUnit {
    fn tap_adjust_unit(self, unit: arrow::datatypes::TimeUnit) -> IcebergType;
}

impl TapAdjustUnit for IcebergType {
    fn tap_adjust_unit(self, _unit: arrow::datatypes::TimeUnit) -> IcebergType {
        self
    }
}

/// Parse partition values out of a Hive-style directory stack, e.g.
/// `["ts_day=2026-09-17"]` with a day transform → `[PartitionValue::Int(20648)]`.
/// `identity` string partitions take the raw directory value.
pub fn parse_hive_partition_value(
    spec: &PartitionSpec,
    schema: &Schema,
    field: &PartitionField,
    raw: &str,
) -> Result<PartitionValue> {
    let result_type = spec
        .result_types(schema)?
        .into_iter()
        .find(|(f, _)| f.name == field.name)
        .map(|(_, t)| t)
        .context("partition field missing from spec")?;
    match (&result_type, field.transform.as_str()) {
        (IcebergType::Int, "day") => {
            let date = chrono::NaiveDate::parse_from_str(raw, "%Y-%m-%d")
                .with_context(|| format!("bad day partition value {raw:?}"))?;
            Ok(PartitionValue::Int(days_since_epoch(date)))
        }
        (IcebergType::Int, "identity") => Ok(PartitionValue::Int(
            raw.parse::<i32>().context("bad int partition value")? as i64,
        )),
        (IcebergType::Long, _) => Ok(PartitionValue::Long(
            raw.parse::<i64>().context("bad long partition value")?,
        )),
        (IcebergType::String, _) => Ok(PartitionValue::String(raw.to_string())),
        (IcebergType::Boolean, _) => Ok(PartitionValue::Boolean(
            raw.parse::<bool>().context("bad boolean partition value")?,
        )),
        (other, t) => bail!(
            "partition field {} transform {t} over {} is not writable",
            field.name,
            other.spec_name()
        ),
    }
}

/// Default table properties TRACE writes.
pub fn default_properties() -> HashMap<String, String> {
    let mut props = HashMap::new();
    props.insert("write.format.default".to_string(), "parquet".to_string());
    props.insert("write.compression-codec".to_string(), "zstd".to_string());
    props.insert(
        "write.target-file-size-bytes".to_string(),
        "536870912".to_string(), // 512MB — ad_events DDL target
    );
    props.insert(
        spec::MIN_SNAPSHOTS_TO_KEEP_PROP.to_string(),
        spec::DEFAULT_MIN_SNAPSHOTS_TO_KEEP.to_string(),
    );
    props
}
