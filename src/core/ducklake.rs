use crate::core::config::{
    Config,
    project::{DatabaseType, RemoteDatabaseConfig, S3AuthMethod, S3Config, StorageConfig},
};
use anyhow::{Context, Result};
use duckdb::{DuckdbConnectionManager, types::Value};
use r2d2::Pool;
use std::{collections::HashMap, path::Path, sync::Arc};

use super::config::project::DatabaseConfig;

#[derive(Debug, Clone)]
pub enum CatalogConfig {
    Sqlite {
        path: String,
    },
    RemoteDatabase {
        db_type: DatabaseType,
        config: RemoteDatabaseConfig,
    },
}

#[derive(Clone)]
pub struct DuckLake {
    catalog_config: CatalogConfig,
    storage_config: StorageConfig,
    pool: Arc<Pool<DuckdbConnectionManager>>,
    _temp_dir: Arc<tempfile::TempDir>,
}

impl DuckLake {
    pub async fn new(catalog_config: CatalogConfig, storage_config: StorageConfig) -> Result<Self> {
        let temp_dir = tempfile::tempdir()?;
        let temp_db_path = temp_dir.path().join("shared.db");

        let manager = DuckdbConnectionManager::file(&temp_db_path)?;
        let pool = Pool::builder()
            .max_size(num_cpus::get() as u32)
            .build(manager)?;

        let instance = Self {
            catalog_config,
            storage_config,
            pool: Arc::new(pool),
            _temp_dir: Arc::new(temp_dir),
        };

        instance.initialize().await?;
        Ok(instance)
    }

    pub async fn from_config(config: &Config) -> Result<DuckLake> {
        let catalog_config = match &config.project.database.ty {
            DatabaseType::Sqlite => CatalogConfig::Sqlite {
                path: config
                    .project
                    .database
                    .path
                    .as_ref()
                    .expect("SQLite database path is required")
                    .clone(),
            },
            DatabaseType::Mysql => {
                let remote_config = build_remote_database_config(&config.project.database)?;
                CatalogConfig::RemoteDatabase {
                    db_type: DatabaseType::Mysql,
                    config: remote_config,
                }
            }
            DatabaseType::Postgresql => {
                let remote_config = build_remote_database_config(&config.project.database)?;
                CatalogConfig::RemoteDatabase {
                    db_type: DatabaseType::Postgresql,
                    config: remote_config,
                }
            }
        };

        DuckLake::new(catalog_config, config.project.storage.clone()).await
    }

    async fn initialize(&self) -> Result<()> {
        self.initialize_base().await?;
        self.attach().await?;
        Ok(())
    }

    async fn initialize_base(&self) -> Result<()> {
        self.execute_batch("INSTALL ducklake; LOAD ducklake;")
            .context("Failed to install and load extensions")?;
        Ok(())
    }

    pub async fn configure_s3_connection(&self, s3_config: &S3Config) -> Result<()> {
        self.create_or_replace_s3_secret("s3_secret", s3_config)
            .await
    }

    async fn attach(&self) -> Result<()> {
        let (extension_sql, attach_sql) = self.catalog_sql()?;

        match &self.storage_config {
            StorageConfig::LocalFile { path } => {
                std::fs::create_dir_all(path)
                    .with_context(|| format!("Failed to create storage directory: {path}"))?;
            }
            StorageConfig::S3(_) => {
                self.configure_s3_storage().await?;
            }
        };

        self.execute_batch(&extension_sql)
            .context("Failed to install and load database extension")?;

        self.execute_batch(&attach_sql)
            .context("Failed to attach DuckLake catalog")?;

        Ok(())
    }

    fn catalog_sql(&self) -> Result<(String, String)> {
        match &self.catalog_config {
            CatalogConfig::Sqlite { path } => {
                let catalog_path = Path::new(path);
                if let Some(parent) = catalog_path.parent() {
                    std::fs::create_dir_all(parent)
                        .context("Failed to create catalog directory")?;
                }

                let data_path = self.get_storage_path();

                let extension_sql = "INSTALL sqlite; LOAD sqlite;".to_string();
                let attach_sql = format!(
                    "ATTACH 'ducklake:sqlite:{path}' AS db (DATA_PATH '{data_path}'); USE db;"
                );

                Ok((extension_sql, attach_sql))
            }
            CatalogConfig::RemoteDatabase { db_type, config } => {
                let data_path = self.get_storage_path();

                let (extension_name, connection_string) = match db_type {
                    DatabaseType::Mysql => {
                        let extension = "mysql";
                        let password = config.password.plaintext()?;
                        let conn_str = format!(
                            "ducklake:mysql:db={} host={} port={} user={} password={}",
                            config.database, config.host, config.port, config.username, password
                        );
                        (extension, conn_str)
                    }
                    DatabaseType::Postgresql => {
                        let extension = "postgres";
                        let password = config.password.plaintext()?;
                        let conn_str = format!(
                            "ducklake:postgres:dbname={} host={} port={} user={} password={}",
                            config.database, config.host, config.port, config.username, password
                        );
                        (extension, conn_str)
                    }
                    DatabaseType::Sqlite => {
                        unreachable!("SQLite should not use RemoteDatabase catalog variant")
                    }
                };

                let extension_sql = format!("INSTALL {extension_name}; LOAD {extension_name};");
                let attach_sql = format!(
                    "ATTACH '{connection_string}' AS db (DATA_PATH '{data_path}', METADATA_SCHEMA '{}_metadata'); USE db;",
                    config.database
                );

                Ok((extension_sql, attach_sql))
            }
        }
    }

    pub fn execute_batch(&self, sql: &str) -> Result<()> {
        let connection = self
            .pool
            .get()
            .context("Failed to get connection from pool")?;

        connection
            .execute_batch(sql)
            .with_context(|| format!("Failed to execute batch SQL: {sql}"))
    }

    pub fn to_string(value: Value) -> String {
        match value {
            Value::Null => "NULL".to_string(),
            Value::Boolean(b) => b.to_string(),
            Value::TinyInt(i) => i.to_string(),
            Value::SmallInt(i) => i.to_string(),
            Value::Int(i) => i.to_string(),
            Value::BigInt(i) => i.to_string(),
            Value::HugeInt(i) => i.to_string(),
            Value::UTinyInt(i) => i.to_string(),
            Value::USmallInt(i) => i.to_string(),
            Value::UInt(i) => i.to_string(),
            Value::UBigInt(i) => i.to_string(),
            Value::Float(f) => f.to_string(),
            Value::Double(f) => f.to_string(),
            Value::Decimal(d) => d.to_string(),
            Value::Text(s) => s,
            Value::Blob(b) => format!("{b:?}"),
            Value::Date32(d) => d.to_string(),
            Value::Time64(_, t) => t.to_string(),
            Value::Timestamp(_, t) => t.to_string(),
            Value::Interval {
                months,
                days,
                nanos,
            } => {
                format!("Interval({months} months, {days} days, {nanos} nanos)")
            }
            Value::List(values) => format!("{values:?}"),
            Value::Enum(s) => s,
            Value::Struct(ordered_map) => format!("{ordered_map:?}"),
            Value::Array(values) => format!("{values:?}"),
            Value::Map(ordered_map) => format!("{ordered_map:?}"),
            Value::Union(value) => format!("{value:?}"),
        }
    }

    pub fn query(&self, sql: &str) -> Result<Vec<Vec<String>>> {
        let connection = self
            .pool
            .get()
            .context("Failed to get connection from pool")?;
        let mut stmt = connection.prepare(sql)?;
        let mut rows = stmt.query([])?;
        let column_count = rows.as_ref().unwrap().column_count();

        let mut results = Vec::new();
        while let Some(row) = rows.next()? {
            let mut row_data = Vec::new();
            for i in 0..column_count {
                let string_value = Self::to_string(row.get(i)?);
                row_data.push(string_value);
            }
            results.push(row_data);
        }

        Ok(results)
    }

    pub fn query_with_column_names(&self, sql: &str) -> Result<HashMap<String, Vec<String>>> {
        let connection = self
            .pool
            .get()
            .context("Failed to get connection from pool")?;

        let column_names = {
            let mut stmt = connection.prepare(sql)?;
            let mut rows = stmt.query([])?;
            let _ = rows.next()?; // Execute once to get column info
            stmt.column_names()
        };

        let mut stmt = connection.prepare(sql)?;
        let mut rows = stmt.query([])?;

        let mut data: HashMap<String, Vec<String>> = HashMap::new();

        for column_name in &column_names {
            data.insert(column_name.clone(), Vec::new());
        }

        while let Some(row) = rows.next()? {
            for (i, column_name) in column_names.iter().enumerate() {
                let string_value = Self::to_string(row.get(i)?);
                data.get_mut(column_name).unwrap().push(string_value);
            }
        }

        Ok(data)
    }

    pub fn create_table_from_query(&self, table_name: &str, query: &str) -> Result<()> {
        let sql = format!("CREATE OR REPLACE TABLE {table_name} AS ({query});");
        self.execute_batch(&sql)
            .with_context(|| format!("Failed to create table '{table_name}' from query: '{query}'"))
    }

    pub fn create_table(&self, table_name: &str, columns: &[(String, String)]) -> Result<()> {
        if columns.is_empty() {
            return Err(anyhow::anyhow!(
                "Cannot create table without column definitions"
            ));
        }

        let column_definitions: Vec<String> = columns
            .iter()
            .map(|(name, data_type)| format!("{name} {data_type}"))
            .collect();

        let columns_sql = column_definitions.join(", ");
        let sql = format!("CREATE OR REPLACE TABLE {table_name} ({columns_sql});");

        self.execute_batch(&sql)
            .with_context(|| format!("Failed to create empty table '{table_name}'"))
    }

    pub fn generate_temp_table_name(prefix: &str) -> String {
        format!("{}_{}", prefix, uuid::Uuid::new_v4().simple())
    }

    pub fn drop_temp_table(&self, table_name: &str) -> Result<()> {
        let drop_sql = format!("DROP TABLE IF EXISTS {table_name};");
        self.execute_batch(&drop_sql)
            .with_context(|| format!("Failed to drop temporary table: {table_name}"))
    }

    pub fn table_exists(&self, table_name: &str) -> Result<bool> {
        let sql = format!(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{table_name}'"
        );
        let results = self.query(&sql)?;
        let exists = if let Some(row) = results.first() {
            if let Some(count_str) = row.first() {
                count_str.parse::<i64>().unwrap_or(0) > 0
            } else {
                false
            }
        } else {
            false
        };
        Ok(exists)
    }

    pub fn table_schema(&self, table_name: &str) -> Result<Vec<(String, String)>> {
        let sql = format!(
            "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{table_name}' ORDER BY ordinal_position"
        );
        let results = self.query(&sql)?;
        let columns = results
            .into_iter()
            .filter_map(|row| {
                if row.len() >= 2 {
                    Some((row[0].clone(), row[1].clone()))
                } else {
                    None
                }
            })
            .collect();
        Ok(columns)
    }

    async fn configure_s3_storage(&self) -> Result<()> {
        if let StorageConfig::S3(s3_config) = &self.storage_config {
            self.ensure_s3_extensions().await?;

            match &s3_config.auth_method {
                S3AuthMethod::CredentialChain => {
                    self.execute_batch("INSTALL aws; LOAD aws;")
                        .context("Failed to install and load aws extension for credential chain")?;
                }
                S3AuthMethod::Explicit => {}
            }

            let secret_sql = Self::build_s3_secret_sql(s3_config, "s3_secret", true)?;
            self.execute_batch(&secret_sql)
                .context("Failed to create S3 secret")?;
        }

        Ok(())
    }

    fn build_s3_secret_sql(
        s3_config: &S3Config,
        secret_name: &str,
        if_not_exists: bool,
    ) -> Result<String> {
        let is_minio = Self::is_minio_endpoint(&s3_config.endpoint_url);

        let create_clause = if if_not_exists {
            format!("CREATE SECRET IF NOT EXISTS {secret_name}")
        } else {
            format!("CREATE OR REPLACE SECRET {secret_name}")
        };

        let mut sql = match &s3_config.auth_method {
            S3AuthMethod::Explicit => {
                let access_key_id = s3_config.access_key_id.as_ref().ok_or_else(|| {
                    anyhow::anyhow!("access_key_id is required for explicit auth")
                })?;
                let secret_access_key = s3_config
                    .secret_access_key
                    .as_ref()
                    .ok_or_else(|| {
                        anyhow::anyhow!("secret_access_key is required for explicit auth")
                    })?
                    .plaintext()?;
                format!(
                    "{create_clause} (
                        TYPE S3,
                        KEY_ID '{}',
                        SECRET '{}',
                        REGION '{}'",
                    access_key_id, secret_access_key, s3_config.region
                )
            }
            S3AuthMethod::CredentialChain => format!(
                "{create_clause} (
                    TYPE S3,
                    PROVIDER credential_chain,
                    REGION '{}'",
                s3_config.region
            ),
        };

        if let Some(endpoint) = &s3_config.endpoint_url {
            let clean_endpoint = Self::clean_endpoint_url(endpoint);
            sql.push_str(&format!(",\n    ENDPOINT '{clean_endpoint}'"));
        }

        if s3_config.path_style_access || is_minio {
            sql.push_str(",\n    URL_STYLE 'path'");
        }

        if is_minio {
            sql.push_str(",\n    USE_SSL false");
        }

        sql.push_str("\n);");
        Ok(sql)
    }

    async fn ensure_s3_extensions(&self) -> Result<()> {
        self.execute_batch("INSTALL httpfs; LOAD httpfs;")
            .context("Failed to install and load httpfs extension for S3")?;
        Ok(())
    }

    fn is_minio_endpoint(endpoint: &Option<String>) -> bool {
        endpoint
            .as_ref()
            .map(|url| url.contains("localhost") || url.contains("127.0.0.1"))
            .unwrap_or(false)
    }

    fn clean_endpoint_url(endpoint: &str) -> &str {
        endpoint
            .strip_prefix("http://")
            .or_else(|| endpoint.strip_prefix("https://"))
            .unwrap_or(endpoint)
    }

    async fn create_or_replace_s3_secret(
        &self,
        secret_name: &str,
        s3_config: &S3Config,
    ) -> Result<()> {
        self.ensure_s3_extensions().await?;

        match &s3_config.auth_method {
            S3AuthMethod::CredentialChain => {
                self.execute_batch("INSTALL aws; LOAD aws;")
                    .context("Failed to install and load aws extension for credential chain")?;
            }
            S3AuthMethod::Explicit => {}
        }

        let secret_sql = Self::build_s3_secret_sql(s3_config, secret_name, false)?;
        self.execute_batch(&secret_sql)
            .context("Failed to create S3 secret")?;

        Ok(())
    }

    fn get_storage_path(&self) -> String {
        match &self.storage_config {
            StorageConfig::LocalFile { path } => path.clone(),
            StorageConfig::S3(s3_config) => format!("s3://{}/ducklake", s3_config.bucket),
        }
    }
}

fn build_remote_database_config(config: &DatabaseConfig) -> Result<RemoteDatabaseConfig> {
    match config.ty {
        DatabaseType::Mysql | DatabaseType::Postgresql => {
            let host = config
                .host
                .as_ref()
                .context("Database host is required")?
                .clone();
            let port = *config.port.as_ref().expect("Database port is required");
            let database = config
                .database
                .as_ref()
                .context("Database name is required")?
                .clone();
            let username = config
                .username
                .as_ref()
                .context("Database username is required")?
                .clone();
            let password = config
                .password
                .as_ref()
                .context("Database password is required")?
                .clone();

            Ok(RemoteDatabaseConfig {
                host,
                port,
                database,
                username,
                password,
            })
        }
        DatabaseType::Sqlite => unreachable!("SQLite should not use RemoteDatabase"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{adapter::test_helpers::test_encrypted_field, config::project::S3Config};

    #[tokio::test]
    async fn test_ducklake_localfile() {
        use std::fs;

        let test_dir = "/tmp/ducklake_test_query";
        fs::remove_dir_all(test_dir).ok();
        fs::create_dir_all(test_dir).unwrap();

        let catalog_config = CatalogConfig::Sqlite {
            path: format!("{test_dir}/test_catalog.sqlite"),
        };
        let storage_config = StorageConfig::LocalFile {
            path: format!("{test_dir}/test_storage"),
        };

        let ducklake = DuckLake::new(catalog_config, storage_config).await.unwrap();

        let query = "SELECT 1 as id, 'Alice' as name";
        ducklake
            .create_table_from_query("test_table", query)
            .unwrap();

        let results = ducklake.query("SELECT * FROM test_table").unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0], vec!["1", "Alice"]);
    }

    #[tokio::test]
    async fn test_ducklake_s3() {
        use aws_config::Region;
        use aws_sdk_s3::{
            Client,
            config::{Builder, Credentials},
            primitives::ByteStream,
        };
        use std::fs;

        let test_dir = "/tmp/ducklake_test_s3";
        fs::remove_dir_all(test_dir).ok();
        fs::create_dir_all(test_dir).unwrap();

        let catalog_config = CatalogConfig::Sqlite {
            path: format!("{test_dir}/test_s3_catalog.sqlite"),
        };
        let bucket_name = &format!("test-bucket-{}", uuid::Uuid::new_v4().simple());
        let s3_config = S3Config {
            bucket: bucket_name.clone(),
            region: "us-east-1".to_string(),
            endpoint_url: Some("http://localhost:9010".to_string()),
            auth_method: S3AuthMethod::Explicit,
            access_key_id: Some("user".to_string()),
            secret_access_key: Some(test_encrypted_field("password")),

            path_style_access: true,
        };
        let storage_config = StorageConfig::S3(s3_config.clone());

        let credentials = Credentials::new("user", "password", None, None, "test");
        let s3_config_builder = Builder::new()
            .region(Region::new("us-east-1"))
            .credentials_provider(credentials)
            .endpoint_url("http://localhost:9010")
            .force_path_style(true)
            .behavior_version_latest();
        let client = Client::from_conf(s3_config_builder.build());

        client.create_bucket().bucket(bucket_name).send().await.ok();

        let test_data = r#"[
  {"date": "2024-01-01", "product_id": 1, "quantity": 10, "revenue": 100.0},
  {"date": "2024-01-02", "product_id": 2, "quantity": 20, "revenue": 200.0},
  {"date": "2024-01-03", "product_id": 3, "quantity": 30, "revenue": 300.0}
]"#;

        let body = ByteStream::from(test_data.as_bytes().to_vec());
        client
            .put_object()
            .bucket(bucket_name)
            .key("test_sales.json")
            .body(body)
            .send()
            .await
            .expect("Failed to upload test data to MinIO");

        let ducklake = DuckLake::new(catalog_config, storage_config).await.unwrap();

        let query = &format!("SELECT * FROM read_json_auto('s3://{bucket_name}/test_sales.json')");
        ducklake
            .create_table_from_query("s3_test_table", query)
            .unwrap();

        let results = ducklake
            .query("SELECT COUNT(*) FROM s3_test_table")
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0][0], "3");

        let results = ducklake
            .query("SELECT product_id, revenue FROM s3_test_table ORDER BY product_id")
            .unwrap();
        assert_eq!(results.len(), 3);
        assert_eq!(results[0], vec!["1", "100"]);
        assert_eq!(results[1], vec!["2", "200"]);
        assert_eq!(results[2], vec!["3", "300"]);

        client
            .delete_object()
            .bucket(bucket_name)
            .key("test_sales.json")
            .send()
            .await
            .ok();

        client.delete_bucket().bucket(bucket_name).send().await.ok();
    }

    #[tokio::test]
    async fn test_ducklake_s3_credential_chain() {
        let catalog_config = CatalogConfig::Sqlite {
            path: "/tmp/test_s3_credential_chain.sqlite".to_string(),
        };
        let s3_config = S3Config {
            bucket: "test-bucket".to_string(),
            region: "us-east-1".to_string(),
            endpoint_url: None,
            auth_method: S3AuthMethod::CredentialChain,
            access_key_id: None,
            secret_access_key: None,

            path_style_access: false,
        };
        let storage_config = StorageConfig::S3(s3_config.clone());

        let ducklake = DuckLake::new(catalog_config, storage_config).await.unwrap();

        let result = ducklake.configure_s3_connection(&s3_config).await;
        assert!(
            result.is_ok(),
            "AWS credential chain configuration should succeed"
        );
    }

    #[tokio::test]
    async fn test_ducklake_s3_explicit() {
        let catalog_config = CatalogConfig::Sqlite {
            path: "/tmp/test_s3_explicit.sqlite".to_string(),
        };
        let s3_config = S3Config {
            bucket: "test-bucket".to_string(),
            region: "us-east-1".to_string(),
            endpoint_url: Some("http://localhost:9000".to_string()),
            auth_method: S3AuthMethod::Explicit,
            access_key_id: Some("minioaccess".to_string()),
            secret_access_key: Some(test_encrypted_field("miniosecret")),

            path_style_access: true,
        };
        let storage_config = StorageConfig::S3(s3_config.clone());

        let ducklake = DuckLake::new(catalog_config, storage_config).await.unwrap();

        let result = ducklake.configure_s3_connection(&s3_config).await;
        assert!(
            result.is_ok(),
            "MinIO explicit configuration should succeed"
        );
    }

    #[tokio::test]
    async fn test_helper_methods() {
        assert!(DuckLake::is_minio_endpoint(&Some(
            "http://localhost:9000".to_string()
        )));
        assert!(DuckLake::is_minio_endpoint(&Some(
            "https://127.0.0.1:9000".to_string()
        )));
        assert!(!DuckLake::is_minio_endpoint(&Some(
            "https://s3.amazonaws.com".to_string()
        )));
        assert!(!DuckLake::is_minio_endpoint(&None));

        assert_eq!(
            DuckLake::clean_endpoint_url("http://localhost:9000"),
            "localhost:9000"
        );
        assert_eq!(
            DuckLake::clean_endpoint_url("https://s3.amazonaws.com"),
            "s3.amazonaws.com"
        );
        assert_eq!(
            DuckLake::clean_endpoint_url("localhost:9000"),
            "localhost:9000"
        );
    }
}
