use crate::core::{
    config::{
        adapter::{AdapterConfig, ColumnConfig},
        project::S3Config,
    },
    ducklake::DuckLake,
};
use anyhow::Result;
use async_trait::async_trait;
use std::sync::Arc;

pub struct S3FileAdapter {
    ducklake: Arc<DuckLake>,
    adapter_config: AdapterConfig,
    s3config: S3Config,
}

impl S3FileAdapter {
    pub fn new(ducklake: Arc<DuckLake>, adapter_config: AdapterConfig, s3config: S3Config) -> Self {
        Self {
            ducklake,
            adapter_config,
            s3config,
        }
    }

    async fn list_s3_files(&self, pattern: &str) -> Result<Vec<String>> {
        let mut config_loader = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .region(Region::new(self.s3config.region.clone()));

        if let Some(endpoint) = &self.s3config.endpoint_url {
            config_loader = config_loader.endpoint_url(endpoint);
        }

        match &self.s3config.auth_method {
            S3AuthMethod::CredentialChain => {}
            S3AuthMethod::Explicit => {
                let secret_access_key = self
                    .s3config
                    .secret_access_key
                    .as_ref()
                    .ok_or_else(|| {
                        anyhow::anyhow!("secret_access_key is required for explicit auth")
                    })?
                    .plaintext()?;
                let session_token = None;
                config_loader = config_loader.credentials_provider(Credentials::new(
                    self.s3config.access_key_id.as_ref().ok_or_else(|| {
                        anyhow::anyhow!("access_key_id is required for explicit auth")
                    })?,
                    secret_access_key,
                    session_token,
                    None,
                    "duckhub",
                ));
            }
        }

        let aws_config = config_loader.load().await;
        let s3_config_builder = Builder::from(&aws_config);
        let aws_s3_config = if self.s3config.path_style_access {
            s3_config_builder.force_path_style(true).build()
        } else {
            s3_config_builder.build()
        };

        let client = Client::from_conf(aws_s3_config);

        let prefix = extract_prefix_from_pattern(pattern);
        let all_keys = self.list_all_objects_with_prefix(&client, &prefix).await?;

        let matching_objects: Vec<String> = all_keys
            .into_iter()
            .filter(|key| matches_pattern(pattern, key))
            .map(|key| format!("s3://{}/{}", self.s3config.bucket, key))
            .collect();

        Ok(matching_objects)
    }

    async fn list_all_objects_with_prefix(
        &self,
        client: &Client,
        prefix: &str,
    ) -> Result<Vec<String>> {
        let mut all_keys = Vec::new();
        let mut continuation_token = None;

        loop {
            let mut request = client
                .list_objects_v2()
                .bucket(&self.s3config.bucket)
                .prefix(prefix);

            if let Some(token) = continuation_token {
                request = request.continuation_token(token);
            }

            let result = request.send().await.context("Failed to list S3 objects")?;

            if let Some(contents) = result.contents {
                for object in contents {
                    if let Some(key) = object.key {
                        all_keys.push(key);
                    }
                }
            }

            continuation_token = result.next_continuation_token;
            if continuation_token.is_none() {
                break;
            }
        }

        Ok(all_keys)
    }
}

#[async_trait]
impl FileAdapter for S3FileAdapter {
    async fn list_files(&self, path: &str) -> Result<Vec<String>> {
        let files = self.list_s3_files(path).await?;
        Ok(files)
    }

    async fn import_files(&self, table_name: &str, files: &[String]) -> Result<()> {
        if files.is_empty() {
            return Ok(());
        }

        self.ducklake
            .configure_s3_connection(&self.s3config)
            .await?;

        let query = build_import_query(&self.adapter_config, files)?;
        self.ducklake.create_table_from_query(table_name, &query)?;
        Ok(())
    }

    async fn get_file_schema(&self, file_path: &str) -> Result<Vec<ColumnInfo>> {
        let s3_path = if file_path.starts_with("s3://") {
            file_path.to_string()
        } else {
            format!("s3://{}/{}", self.s3config.bucket, file_path)
        };

        self.ducklake
            .configure_s3_connection(&self.s3config)
            .await?;

        let temp_table = format!("temp_schema_check_{}", uuid::Uuid::new_v4().simple());

        match &self.adapter_config.source {
            crate::core::config::adapter::AdapterSource::File { format, .. } => {
                let query = match format.ty.as_str() {
                    "csv" => {
                        let has_header = format.has_header.unwrap_or(true);
                        format!(
                            "CREATE TEMP TABLE {temp_table} AS SELECT * FROM read_csv_auto('{s3_path}', header={has_header}) LIMIT 0"
                        )
                    }
                    "json" => {
                        format!(
                            "CREATE TEMP TABLE {temp_table} AS SELECT * FROM read_json_auto('{s3_path}') LIMIT 0"
                        )
                    }
                    "parquet" => {
                        format!(
                            "CREATE TEMP TABLE {temp_table} AS SELECT * FROM read_parquet('{s3_path}') LIMIT 0"
                        )
                    }
                    _ => return Err(anyhow::anyhow!("Unsupported file format: {}", format.ty)),
                };

                self.ducklake.execute_batch(&query)?;

                let schema_query = format!("DESCRIBE {temp_table}");
                let result = self.ducklake.query(&schema_query)?;

                let _ = self
                    .ducklake
                    .execute_batch(&format!("DROP TABLE {temp_table}"));

                let columns = result
                    .into_iter()
                    .map(|row| ColumnInfo {
                        name: row[0].clone(),
                        data_type: row[1].clone(),
                    })
                    .collect();

                Ok(columns)
            }
            _ => Err(anyhow::anyhow!("Expected file source")),
        }
    }

    async fn validate_schema(
        &self,
        file_path: &str,
        expected_columns: &[ColumnConfig],
    ) -> Result<()> {
        let actual_columns = self.get_file_schema(file_path).await?;

        for expected in expected_columns {
            let found = actual_columns.iter().find(|col| col.name == expected.name);

            match found {
                None => {
                    return Err(anyhow::anyhow!(
                        "Column '{}' not found in file '{}'",
                        expected.name,
                        file_path
                    ));
                }
                Some(actual) => {
                    if !self.types_match(&expected.ty, &actual.data_type) {
                        return Err(anyhow::anyhow!(
                            "Column '{}' type mismatch: expected '{}', found '{}'",
                            expected.name,
                            expected.ty,
                            actual.data_type
                        ));
                    }
                }
            }
        }

        Ok(())
    }
}

impl S3FileAdapter {
    fn types_match(&self, expected: &str, actual: &str) -> bool {
        let normalize_type = |t: &str| -> String {
            t.to_uppercase()
                .replace("INTEGER", "BIGINT")
                .replace("STRING", "VARCHAR")
                .replace("FLOAT", "DOUBLE")
        };

        normalize_type(expected) == normalize_type(actual)
    }
}

use crate::core::config::project::S3AuthMethod;
use anyhow::Context;
use aws_config::Region;
use aws_sdk_s3::{
    Client,
    config::{Builder, Credentials},
};
use regex::Regex;

use super::{super::database::ColumnInfo, FileAdapter, build_import_query};

fn extract_prefix_from_pattern(pattern: &str) -> String {
    let mut prefix = String::new();
    for part in pattern.split('/') {
        if part.contains('*') || part.contains('?') {
            break;
        }
        if !prefix.is_empty() {
            prefix.push('/');
        }
        prefix.push_str(part);
    }
    prefix
}

fn matches_pattern(pattern: &str, key: &str) -> bool {
    let pattern_regex = pattern
        .replace(".", "\\.")
        .replace("*", ".*")
        .replace("?", ".");

    if let Ok(regex) = Regex::new(&format!("^{pattern_regex}$")) {
        regex.is_match(key)
    } else {
        false
    }
}
