//! Iceberg REST catalog publisher used by TRACE's Parquet producers.
//!
//! Data objects are uploaded first, then appended through the catalog. The
//! Apache Iceberg implementation writes Avro manifests and commits a new
//! snapshot using the REST catalog's optimistic concurrency protocol.

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use anyhow::{bail, ensure, Context, Result};
use chrono::NaiveDate;
use futures_util::StreamExt;
use iceberg_catalog_rest::{
    RestCatalogBuilder, REST_CATALOG_PROP_URI, REST_CATALOG_PROP_WAREHOUSE,
};
use iceberg_rust::{
    spec::{
        DataContentType, DataFileBuilder, DataFileFormat, FormatVersion, ListType, Literal,
        MapType, MappedField, NameMapping, NestedField, PrimitiveType, Schema, Struct, Transform,
        Type, UnboundPartitionSpec, DEFAULT_SCHEMA_NAME_MAPPING,
    },
    table::Table,
    transaction::{ApplyTransactionAction, Transaction},
    Catalog, CatalogBuilder, NamespaceIdent, TableCreation, TableIdent,
};
use iceberg_storage_opendal::OpenDalStorageFactory;
use serde_json::{json, Value};

use crate::types::{Field, IcebergType as TraceType, Schema as TraceSchema};

/// S3 and catalog settings shared by the flusher and compactor.
#[derive(Clone, Debug)]
pub struct PublisherConfig {
    pub catalog_uri: String,
    pub warehouse: String,
    pub bucket: String,
    pub prefix: String,
    pub region: String,
    pub endpoint: Option<String>,
    pub access_key_id: Option<String>,
    pub secret_access_key: Option<String>,
}

impl PublisherConfig {
    /// Read the shared deployment variables. Publication is an explicit
    /// cutover flag so historical Parquet can be migrated before producers
    /// start appending to the catalog.
    pub fn from_env() -> Result<Option<Self>> {
        if std::env::var("TRACE_ICEBERG_PUBLISH")
            .map(|value| !value.eq_ignore_ascii_case("true"))
            .unwrap_or(true)
        {
            return Ok(None);
        }
        let catalog_uri = std::env::var("ICEBERG_CATALOG_URI").ok();
        let warehouse = std::env::var("ICEBERG_WAREHOUSE").ok();
        match (catalog_uri, warehouse) {
            (None, None) => Ok(None),
            (Some(catalog_uri), Some(warehouse)) => {
                let bucket = std::env::var("TRACE_S3_BUCKET")
                    .context("TRACE_S3_BUCKET is required with Iceberg enabled")?;
                Ok(Some(Self {
                    catalog_uri,
                    warehouse,
                    bucket,
                    prefix: std::env::var("TRACE_S3_PREFIX")
                        .unwrap_or_else(|_| "trace-events".to_string()),
                    region: std::env::var("TRACE_S3_REGION")
                        .unwrap_or_else(|_| "us-east-1".to_string()),
                    endpoint: std::env::var("TRACE_S3_ENDPOINT").ok(),
                    access_key_id: std::env::var("AWS_ACCESS_KEY_ID").ok(),
                    secret_access_key: std::env::var("AWS_SECRET_ACCESS_KEY").ok(),
                }))
            }
            _ => bail!("ICEBERG_CATALOG_URI and ICEBERG_WAREHOUSE must be set together"),
        }
    }

    /// Resolve a key returned by TRACE's S3 client to its complete object URI.
    pub fn object_uri(&self, key: &str) -> String {
        format!(
            "s3://{}/{}/{}",
            self.bucket,
            self.prefix.trim_matches('/'),
            key.trim_start_matches('/')
        )
    }
}

/// Metadata for a Parquet object already uploaded to S3.
#[derive(Clone, Debug)]
pub struct ParquetObject {
    /// Full `s3://` URI, as recorded in Iceberg manifests.
    pub location: String,
    pub size_bytes: u64,
    pub record_count: u64,
    /// UTC day represented by the table's `day(timestamp)` transform.
    pub partition_day: Option<NaiveDate>,
    /// Network represented by the campaigns table's identity transform.
    pub partition_network: Option<String>,
}

/// Return the data-file URIs currently referenced by an existing table.
/// Callers must not mutate or delete these objects outside an Iceberg rewrite.
pub async fn registered_data_file_locations(
    config: &PublisherConfig,
    table_name: &str,
) -> Result<HashSet<String>> {
    let catalog = rest_catalog(config).await?;
    let ident = TableIdent::new(
        NamespaceIdent::new("trace".to_string()),
        table_name.to_string(),
    );
    if !catalog.table_exists(&ident).await? {
        return Ok(HashSet::new());
    }
    let table = catalog.load_table(&ident).await?;
    data_file_locations(&table).await
}

async fn data_file_locations(table: &iceberg_rust::table::Table) -> Result<HashSet<String>> {
    let mut paths = HashSet::new();
    let mut stream = table
        .scan()
        .build()
        .context("build scan for existing Iceberg files")?
        .plan_files()
        .await
        .context("list existing Iceberg files")?;
    while let Some(task) = stream.next().await {
        paths.insert(task?.data_file_path);
    }
    Ok(paths)
}

/// Append existing Parquet data files to a catalog table and commit a snapshot.
///
/// TRACE's event and session files have day partition directories; campaign
/// files have network partition directories.
/// The table schema is derived from the producer's actual Parquet footer so
/// the catalog schema tracks the files that are appended.
pub async fn append_parquet_files(
    config: &PublisherConfig,
    table_name: &str,
    parquet_schema_bytes: &[u8],
    objects: Vec<ParquetObject>,
) -> Result<i64> {
    if objects.is_empty() {
        return Ok(0);
    }

    let (trace_schema, _) = crate::types::schema_from_parquet(parquet_schema_bytes)
        .context("derive Iceberg schema from Parquet footer")?;
    let schema = to_apache_schema(&trace_schema)?;
    let partition_column = partition_column(table_name);
    let source_partition_id = partition_column
        .and_then(|name| trace_schema.fields.iter().find(|field| field.name == name))
        .map(|field| field.id);

    if let Some(column) = partition_column {
        if source_partition_id.is_none() {
            bail!("table {table_name} requires partition source column {column}");
        }
    }

    let catalog = rest_catalog(config).await?;
    let namespace = NamespaceIdent::new("trace".to_string());
    if !catalog.namespace_exists(&namespace).await? {
        catalog
            .create_namespace(&namespace, HashMap::new())
            .await
            .context("create Iceberg namespace trace")?;
    }

    let ident = TableIdent::new(namespace.clone(), table_name.to_string());
    let table = if catalog.table_exists(&ident).await? {
        let mut existing = catalog.load_table(&ident).await?;
        ensure_schema_compatible(&trace_schema, existing.metadata().current_schema())?;
        if table_name == "campaigns" {
            existing = ensure_campaign_name_mapping(&catalog, existing, &trace_schema).await?;
        }
        existing
    } else {
        let partition_spec = match (partition_column, source_partition_id) {
            (Some(_), Some(source_id)) => Some(
                UnboundPartitionSpec::builder()
                    .add_partition_field(
                        source_id,
                        partition_field_name(table_name).unwrap(),
                        partition_transform(table_name).unwrap(),
                    )
                    .context("build Iceberg partition spec")?
                    .build(),
            ),
            _ => None,
        };
        let table_location = format!(
            "{}/{}/{}",
            config.warehouse.trim_end_matches('/'),
            namespace,
            table_name
        );
        let creation = TableCreation::builder()
            .name(table_name.to_string())
            .location(table_location)
            .schema(schema.clone())
            .partition_spec_opt(partition_spec)
            .properties(HashMap::from([
                ("write.format.default".to_string(), "parquet".to_string()),
                ("write.compression-codec".to_string(), "zstd".to_string()),
                (
                    "schema.name-mapping.default".to_string(),
                    name_mapping_json(&trace_schema).to_string(),
                ),
                (
                    "history.expire.min-snapshots-to-keep".to_string(),
                    "10".to_string(),
                ),
            ]))
            .format_version(FormatVersion::V2)
            .build();
        catalog
            .create_table(&namespace, creation)
            .await
            .with_context(|| format!("create Iceberg table {ident}"))?
    };

    let spec = table.metadata().default_partition_spec();
    let spec_id = spec.spec_id();
    let existing_paths = data_file_locations(&table).await?;

    let mut data_files = Vec::with_capacity(objects.len());
    for object in objects {
        if existing_paths.contains(&object.location) {
            continue;
        }
        let partition = object_partition(table_name, spec.fields(), &object)?;

        let file = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path(object.location)
            .file_format(DataFileFormat::Parquet)
            .partition(partition)
            .partition_spec_id(spec_id)
            .record_count(object.record_count)
            .file_size_in_bytes(object.size_bytes)
            .build()
            .context("build Iceberg data-file manifest entry")?;
        data_files.push(file);
    }

    if data_files.is_empty() {
        return Ok(0);
    }

    let count = data_files.len() as i64;
    let tx = Transaction::new(&table);
    tx.fast_append()
        .set_snapshot_properties(HashMap::from([(
            "trace.publisher".to_string(),
            "flusher-compactor".to_string(),
        )]))
        .add_data_files(data_files)
        .apply(tx)?
        .commit(&catalog)
        .await
        .with_context(|| format!("commit Iceberg snapshot for {ident}"))?;
    Ok(count)
}

fn partition_column(table_name: &str) -> Option<&'static str> {
    match table_name {
        "ad_events" => Some("ts"),
        "sessions" => Some("started_at"),
        "campaigns" => Some("network"),
        _ => None,
    }
}

fn partition_field_name(table_name: &str) -> Option<&'static str> {
    match table_name {
        "ad_events" => Some("ts_day"),
        "sessions" => Some("started_at_day"),
        "campaigns" => Some("network"),
        _ => None,
    }
}

fn partition_transform(table_name: &str) -> Option<Transform> {
    match table_name {
        "ad_events" | "sessions" => Some(Transform::Day),
        "campaigns" => Some(Transform::Identity),
        _ => None,
    }
}

fn object_partition(
    table_name: &str,
    fields: &[iceberg_rust::spec::PartitionField],
    object: &ParquetObject,
) -> Result<Struct> {
    if table_name == "campaigns" {
        ensure!(
            fields.len() == 1
                && fields[0].name == "network"
                && fields[0].transform == Transform::Identity,
            "campaigns table must be identity-partitioned by network"
        );
        let network = object
            .partition_network
            .as_deref()
            .context("network-partitioned Iceberg object is missing partition_network")?;
        ensure!(!network.is_empty(), "campaign network partition is empty");
        return Ok(Struct::from_iter([Some(Literal::string(network))]));
    }
    if fields.is_empty() {
        return Ok(Struct::empty());
    }
    let day = object
        .partition_day
        .context("day-partitioned Iceberg object is missing partition_day")?;
    let days_since_epoch = day
        .signed_duration_since(NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
        .num_days();
    let days_since_epoch =
        i32::try_from(days_since_epoch).context("partition day is outside Iceberg date range")?;
    Ok(Struct::from_iter([Some(Literal::date(days_since_epoch))]))
}

fn mapped_field(field: &NestedField) -> MappedField {
    let children = match field.field_type.as_ref() {
        Type::Struct(value) => value
            .fields()
            .iter()
            .map(|child| mapped_field(child.as_ref()))
            .collect(),
        Type::Map(value) => vec![
            mapped_field(value.key_field.as_ref()),
            mapped_field(value.value_field.as_ref()),
        ],
        Type::List(value) => vec![mapped_field(value.element_field.as_ref())],
        Type::Primitive(_) => Vec::new(),
    };
    MappedField::new(Some(field.id), vec![field.name.clone()], children)
}

fn mapping_from_existing_schema(schema: &Schema) -> NameMapping {
    NameMapping::new(
        schema
            .as_struct()
            .fields()
            .iter()
            .map(|field| mapped_field(field.as_ref()))
            .collect(),
    )
}

async fn ensure_campaign_name_mapping(
    catalog: &dyn Catalog,
    table: Table,
    source: &TraceSchema,
) -> Result<Table> {
    let target = table.metadata().current_schema();
    if let Some(value) = table
        .metadata()
        .properties()
        .get(DEFAULT_SCHEMA_NAME_MAPPING)
    {
        let mapping: NameMapping =
            serde_json::from_str(value).context("parse existing campaigns Iceberg name mapping")?;
        for field in &source.fields {
            let target_id = target
                .field_by_name(&field.name)
                .context("campaign schema is missing Parquet field")?
                .id;
            ensure!(
                mapping.fields().iter().any(|mapped| {
                    mapped.names().iter().any(|name| name == &field.name)
                        && mapped.field_id() == Some(target_id)
                }),
                "campaign name mapping does not map {} to Iceberg field ID {}",
                field.name,
                target_id
            );
        }
        return Ok(table);
    }

    let mapping = mapping_from_existing_schema(target);
    let value =
        serde_json::to_string(&mapping).context("serialize campaigns Iceberg name mapping")?;
    let tx = Transaction::new(&table);
    tx.update_table_properties()
        .set(DEFAULT_SCHEMA_NAME_MAPPING.to_string(), value)
        .apply(tx)?
        .commit(catalog)
        .await
        .context("set campaigns Iceberg name mapping before appending Parquet")
}

async fn rest_catalog(config: &PublisherConfig) -> Result<iceberg_catalog_rest::RestCatalog> {
    let mut props = HashMap::from([
        (
            REST_CATALOG_PROP_URI.to_string(),
            config.catalog_uri.clone(),
        ),
        (
            REST_CATALOG_PROP_WAREHOUSE.to_string(),
            config.warehouse.clone(),
        ),
        ("s3.region".to_string(), config.region.clone()),
        ("s3.path-style-access".to_string(), "true".to_string()),
    ]);
    if let Some(endpoint) = &config.endpoint {
        props.insert("s3.endpoint".to_string(), endpoint.clone());
        props.insert("s3.allow-http".to_string(), "true".to_string());
    }
    if let Some(access_key) = &config.access_key_id {
        props.insert("s3.access-key-id".to_string(), access_key.clone());
    }
    if let Some(secret_key) = &config.secret_access_key {
        props.insert("s3.secret-access-key".to_string(), secret_key.clone());
    }
    RestCatalogBuilder::default()
        .with_storage_factory(Arc::new(OpenDalStorageFactory::S3 {
            customized_credential_load: None,
        }))
        .load("trace", props)
        .await
        .context("load Iceberg REST catalog")
}

fn ensure_schema_compatible(
    source: &TraceSchema,
    target: &iceberg_rust::spec::Schema,
) -> Result<()> {
    let expected_schema = to_apache_schema(source)?;
    for field in &source.fields {
        let target_field = target
            .field_by_name(&field.name)
            .with_context(|| format!("Iceberg table is missing Parquet field {}", field.name))?;
        let expected = &expected_schema
            .field_by_name(&field.name)
            .context("converted Iceberg schema lost a source field")?
            .field_type;
        if !iceberg_type_compatible(expected.as_ref(), target_field.field_type.as_ref()) {
            bail!(
                "Iceberg column {} has type {}, Parquet has incompatible type {}",
                field.name,
                target_field.field_type,
                expected
            );
        }
    }
    Ok(())
}

fn iceberg_type_compatible(source: &Type, target: &Type) -> bool {
    use iceberg_rust::spec::PrimitiveType;
    match (source, target) {
        (Type::Primitive(PrimitiveType::Int), Type::Primitive(PrimitiveType::Long))
        | (Type::Primitive(PrimitiveType::Float), Type::Primitive(PrimitiveType::Double)) => true,
        (Type::Map(source), Type::Map(target)) => {
            iceberg_type_compatible(
                source.key_field.field_type.as_ref(),
                target.key_field.field_type.as_ref(),
            ) && iceberg_type_compatible(
                source.value_field.field_type.as_ref(),
                target.value_field.field_type.as_ref(),
            )
        }
        (Type::List(source), Type::List(target)) => iceberg_type_compatible(
            source.element_field.field_type.as_ref(),
            target.element_field.field_type.as_ref(),
        ),
        _ => source == target,
    }
}

fn to_apache_schema(source: &TraceSchema) -> Result<Schema> {
    let mut nested_id = source.highest_field_id() + 1;
    let mut fields = Vec::with_capacity(source.fields.len());
    for field in &source.fields {
        let field_type = to_apache_type(&field.field_type, nested_id)?;
        nested_id += 100;
        let nested = if field.required {
            NestedField::required(field.id, &field.name, field_type)
        } else {
            NestedField::optional(field.id, &field.name, field_type)
        };
        fields.push(nested.into());
    }
    Schema::builder()
        .with_fields(fields)
        .with_schema_id(source.schema_id)
        .build()
        .context("build Apache Iceberg schema")
}

fn to_apache_type(source: &TraceType, nested_id: i32) -> Result<Type> {
    let primitive = |ty| Type::Primitive(ty);
    Ok(match source {
        TraceType::Boolean => primitive(PrimitiveType::Boolean),
        TraceType::Int => primitive(PrimitiveType::Int),
        TraceType::Long => primitive(PrimitiveType::Long),
        TraceType::Float => primitive(PrimitiveType::Float),
        TraceType::Double => primitive(PrimitiveType::Double),
        TraceType::String => primitive(PrimitiveType::String),
        TraceType::Timestamp => primitive(PrimitiveType::Timestamp),
        TraceType::Timestamptz => primitive(PrimitiveType::Timestamptz),
        TraceType::Date => primitive(PrimitiveType::Date),
        TraceType::Map(key, value) => Type::Map(MapType::optional(
            nested_id,
            to_apache_type(key, nested_id + 1)?,
            nested_id + 1,
            to_apache_type(value, nested_id + 2)?,
        )),
        TraceType::List(element) => Type::List(ListType::new(
            NestedField::optional(
                nested_id,
                "element",
                to_apache_type(element, nested_id + 1)?,
            )
            .into(),
        )),
    })
}

fn name_mapping_json(schema: &TraceSchema) -> Value {
    let mut next_nested_id = schema.highest_field_id() + 1;
    let entries: Vec<Value> = schema
        .fields
        .iter()
        .map(|field| {
            let id = next_nested_id;
            next_nested_id += 100;
            name_mapping_field(field, id)
        })
        .collect();
    entries.into()
}

fn name_mapping_field(field: &Field, nested_id: i32) -> Value {
    let mut mapping = json!({"field-id": field.id, "names": [field.name]});
    match &field.field_type {
        TraceType::Map(_, _) => {
            mapping["fields"] = json!([
                {"field-id": nested_id, "names": ["key"]},
                {"field-id": nested_id + 1, "names": ["value"]}
            ]);
        }
        TraceType::List(_) => {
            mapping["fields"] = json!([
                {"field-id": nested_id, "names": ["element"]}
            ]);
        }
        _ => {}
    }
    mapping
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{Field, IcebergType, Schema as TraceIcebergSchema};
    use iceberg_rust::{
        memory::{MemoryCatalogBuilder, MEMORY_CATALOG_WAREHOUSE},
        spec::DataFileBuilder,
    };

    #[test]
    fn maps_trace_nested_types_into_iceberg_schema_and_name_mapping() {
        let trace_schema = TraceIcebergSchema::new(
            vec![
                Field {
                    id: 1,
                    name: "ts".to_string(),
                    required: true,
                    field_type: IcebergType::Timestamp,
                    doc: None,
                },
                Field {
                    id: 2,
                    name: "params".to_string(),
                    required: false,
                    field_type: IcebergType::Map(
                        Box::new(IcebergType::String),
                        Box::new(IcebergType::String),
                    ),
                    doc: None,
                },
            ],
            None,
        );
        let schema = to_apache_schema(&trace_schema).unwrap();
        assert_eq!(schema.field_by_name("ts").unwrap().id, 1);
        assert!(matches!(
            schema.field_by_name("params").unwrap().field_type.as_ref(),
            Type::Map(_)
        ));
        assert_eq!(name_mapping_json(&trace_schema)[1]["field-id"], 2);
        assert_eq!(partition_column("ad_events"), Some("ts"));
        assert_eq!(partition_field_name("sessions"), Some("started_at_day"));
        assert_eq!(partition_column("campaigns"), Some("network"));
        assert_eq!(partition_field_name("campaigns"), Some("network"));
        assert_eq!(partition_transform("campaigns"), Some(Transform::Identity));
    }

    #[test]
    fn campaign_manifest_partition_is_network_identity() {
        let field = iceberg_rust::spec::PartitionField {
            source_id: 2,
            field_id: 1000,
            name: "network".to_string(),
            transform: Transform::Identity,
        };
        let object = ParquetObject {
            location: "s3://bucket/iceberg/campaigns/data/network=taboola/part.parquet".to_string(),
            size_bytes: 100,
            record_count: 1,
            partition_day: None,
            partition_network: Some("taboola".to_string()),
        };
        assert_eq!(
            object_partition("campaigns", &[field.clone()], &object).unwrap(),
            Struct::from_iter([Some(Literal::string("taboola"))])
        );

        let mut missing = object.clone();
        missing.partition_network = None;
        assert!(object_partition("campaigns", &[field.clone()], &missing).is_err());
        assert!(object_partition("campaigns", &[], &object).is_err());
        let mut wrong_transform = field;
        wrong_transform.transform = Transform::Day;
        assert!(object_partition("campaigns", &[wrong_transform], &object).is_err());
    }

    #[test]
    fn day_manifest_partition_still_uses_days_since_epoch() {
        let field = iceberg_rust::spec::PartitionField {
            source_id: 1,
            field_id: 1000,
            name: "started_at_day".to_string(),
            transform: Transform::Day,
        };
        let day = NaiveDate::from_ymd_opt(2026, 9, 14).unwrap();
        let object = ParquetObject {
            location: "s3://bucket/iceberg/sessions/data/started_at_day=2026-09-14/part.parquet"
                .to_string(),
            size_bytes: 100,
            record_count: 1,
            partition_day: Some(day),
            partition_network: None,
        };
        let days = day
            .signed_duration_since(NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
            .num_days() as i32;
        assert_eq!(
            object_partition("sessions", &[field], &object).unwrap(),
            Struct::from_iter([Some(Literal::date(days))])
        );
    }

    #[tokio::test]
    async fn existing_campaign_table_gets_name_mapping_from_its_own_field_ids() {
        let catalog = MemoryCatalogBuilder::default()
            .load(
                "test",
                HashMap::from([(
                    MEMORY_CATALOG_WAREHOUSE.to_string(),
                    "memory://warehouse".to_string(),
                )]),
            )
            .await
            .unwrap();
        let namespace = NamespaceIdent::new("trace".to_string());
        catalog
            .create_namespace(&namespace, HashMap::new())
            .await
            .unwrap();
        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::required(41, "campaign_id", Type::Primitive(PrimitiveType::String))
                    .into(),
                NestedField::required(73, "network", Type::Primitive(PrimitiveType::String)).into(),
            ])
            .build()
            .unwrap();
        let table = catalog
            .create_table(
                &namespace,
                TableCreation::builder()
                    .name("campaigns".to_string())
                    .location("memory://warehouse/trace/campaigns".to_string())
                    .schema(schema)
                    .build(),
            )
            .await
            .unwrap();
        // MemoryCatalog renumbers fields on create. Use different producer
        // IDs to verify the mapping follows the stored table's IDs.
        let campaign_id = table
            .metadata()
            .current_schema()
            .field_by_name("campaign_id")
            .unwrap()
            .id;
        let network_id = table
            .metadata()
            .current_schema()
            .field_by_name("network")
            .unwrap()
            .id;
        let source = TraceIcebergSchema::new(
            vec![
                Field {
                    id: 101,
                    name: "campaign_id".to_string(),
                    required: true,
                    field_type: IcebergType::String,
                    doc: None,
                },
                Field {
                    id: 102,
                    name: "network".to_string(),
                    required: true,
                    field_type: IcebergType::String,
                    doc: None,
                },
            ],
            None,
        );
        let updated = ensure_campaign_name_mapping(&catalog, table, &source)
            .await
            .unwrap();
        let value = updated
            .metadata()
            .properties()
            .get(DEFAULT_SCHEMA_NAME_MAPPING)
            .unwrap();
        let mapping: NameMapping = serde_json::from_str(value).unwrap();
        assert_ne!(campaign_id, 101);
        assert_ne!(network_id, 102);
        assert_eq!(mapping.fields()[0].field_id(), Some(campaign_id));
        assert_eq!(mapping.fields()[1].field_id(), Some(network_id));
        ensure_campaign_name_mapping(&catalog, updated, &source)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn apache_catalog_commits_valid_snapshots_with_time_travel_lineage() {
        use iceberg_rust::spec::{DataContentType, DataFileFormat};

        let catalog = MemoryCatalogBuilder::default()
            .load(
                "test",
                HashMap::from([(
                    MEMORY_CATALOG_WAREHOUSE.to_string(),
                    "memory://warehouse".to_string(),
                )]),
            )
            .await
            .unwrap();
        let namespace = NamespaceIdent::new("trace".to_string());
        catalog
            .create_namespace(&namespace, HashMap::new())
            .await
            .unwrap();
        let schema = Schema::builder()
            .with_fields(vec![NestedField::required(
                1,
                "id",
                Type::Primitive(PrimitiveType::Long),
            )
            .into()])
            .build()
            .unwrap();
        let ident = TableIdent::new(namespace.clone(), "ad_events".to_string());
        let table = catalog
            .create_table(
                &namespace,
                TableCreation::builder()
                    .name("ad_events".to_string())
                    .location("memory://warehouse/trace/ad_events".to_string())
                    .schema(schema)
                    .build(),
            )
            .await
            .unwrap();

        let first_file = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path("memory://warehouse/trace/ad_events/data/first.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .record_count(1)
            .file_size_in_bytes(100)
            .build()
            .unwrap();
        let tx = Transaction::new(&table);
        let first = tx
            .fast_append()
            .add_data_files([first_file])
            .apply(tx)
            .unwrap()
            .commit(&catalog)
            .await
            .unwrap();
        let first_snapshot_id = first.metadata().current_snapshot().unwrap().snapshot_id();

        let second_file = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path("memory://warehouse/trace/ad_events/data/second.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .record_count(1)
            .file_size_in_bytes(100)
            .build()
            .unwrap();
        let tx = Transaction::new(&first);
        let second = tx
            .fast_append()
            .add_data_files([second_file])
            .apply(tx)
            .unwrap()
            .commit(&catalog)
            .await
            .unwrap();

        let snapshots: Vec<_> = second.metadata().snapshots().collect();
        assert_eq!(snapshots.len(), 2);
        assert_eq!(
            second
                .metadata()
                .current_snapshot()
                .unwrap()
                .parent_snapshot_id(),
            Some(first_snapshot_id)
        );
        assert_eq!(
            catalog
                .load_table(&ident)
                .await
                .unwrap()
                .metadata()
                .snapshots()
                .count(),
            2
        );
        let paths = data_file_locations(&catalog.load_table(&ident).await.unwrap())
            .await
            .unwrap();
        assert_eq!(paths.len(), 2);
        assert!(paths.contains("memory://warehouse/trace/ad_events/data/first.parquet"));
        assert!(paths.contains("memory://warehouse/trace/ad_events/data/second.parquet"));
    }
}
