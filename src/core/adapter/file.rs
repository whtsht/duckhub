pub mod localfile;
pub mod s3;

use super::{Adapter, adapter_from_connection, database::ColumnInfo};
use crate::core::config::{
    adapter::{AdapterConfig, AdapterSource, ColumnConfig},
    project::ConnectionConfig,
};
use anyhow::Result;
use async_trait::async_trait;
use localfile::LocalFileAdapter;
use s3::S3FileAdapter;
use std::collections::HashMap;

#[async_trait]
pub trait FileAdapter: Send + Sync {
    async fn list_files(&self, pattern: &str) -> Result<Vec<String>>;
    async fn import_files(&self, table_name: &str, files: &[String]) -> Result<()>;
    async fn get_file_schema(&self, file_path: &str) -> Result<Vec<ColumnInfo>>;
    async fn validate_schema(
        &self,
        file_path: &str,
        expected_columns: &[ColumnConfig],
    ) -> Result<()>;
}

impl Adapter {
    pub async fn file_import(&self, table_name: &str, adapter: Box<dyn FileAdapter>) -> Result<()> {
        let pattern = if let AdapterSource::File { file, .. } = &self.config.source {
            &file.path
        } else {
            return Err(anyhow::anyhow!("Adapter source is not a file"));
        };

        let files = adapter.list_files(pattern).await?;
        adapter.import_files(table_name, &files).await?;

        Ok(())
    }

    pub fn file_adapter(
        &self,
        name: &str,
        connections: &HashMap<String, ConnectionConfig>,
    ) -> Result<Box<dyn FileAdapter>> {
        let connection = adapter_from_connection(name, connections)?;

        match connection {
            ConnectionConfig::LocalFile { base_path, .. } => {
                let adapter = LocalFileAdapter::new(
                    self.ducklake.clone(),
                    self.config.clone(),
                    Some(base_path),
                );
                Ok(Box::new(adapter))
            }
            ConnectionConfig::S3(s3config) => {
                let adapter =
                    S3FileAdapter::new(self.ducklake.clone(), self.config.clone(), s3config);
                Ok(Box::new(adapter))
            }
            _ => Err(anyhow::anyhow!(
                "Unsupported connection type for file adapter"
            )),
        }
    }
}

pub fn build_import_query(adapter_config: &AdapterConfig, files: &[String]) -> Result<String> {
    if files.is_empty() {
        return Err(anyhow::anyhow!("No files to load"));
    }

    if files.len() == 1 {
        let file_path = &files[0];
        return build_import_query_single(adapter_config, file_path);
    }

    let file_paths_str = files
        .iter()
        .map(|p| format!("'{p}'"))
        .collect::<Vec<_>>()
        .join(", ");

    match &adapter_config.source {
        crate::core::config::adapter::AdapterSource::File { format, .. } => {
            match format.ty.as_str() {
                "csv" => {
                    let has_header = format.has_header.unwrap_or(true);
                    let query = format!(
                        "SELECT * FROM read_csv_auto([{file_paths_str}], header={has_header})"
                    );
                    Ok(query)
                }
                "parquet" => {
                    let query = format!("SELECT * FROM read_parquet([{file_paths_str}])");
                    Ok(query)
                }
                "json" => {
                    let query = format!("SELECT * FROM read_json_auto([{file_paths_str}])");
                    Ok(query)
                }
                _ => Err(anyhow::anyhow!("Unsupported format: {}", format.ty)),
            }
        }
        _ => Err(anyhow::anyhow!(
            "Only file sources are supported in file processing"
        )),
    }
}

fn build_import_query_single(adapter_config: &AdapterConfig, file_path: &str) -> Result<String> {
    match &adapter_config.source {
        crate::core::config::adapter::AdapterSource::File { format, .. } => {
            match format.ty.as_str() {
                "csv" => {
                    let has_header = format.has_header.unwrap_or(true);
                    let query =
                        format!("SELECT * FROM read_csv_auto('{file_path}', header={has_header})");
                    Ok(query)
                }
                "parquet" => {
                    let query = format!("SELECT * FROM read_parquet('{file_path}')");
                    Ok(query)
                }
                "json" => {
                    let query = format!("SELECT * FROM read_json_auto('{file_path}')");
                    Ok(query)
                }
                _ => Err(anyhow::anyhow!("Unsupported format: {}", format.ty)),
            }
        }
        _ => Err(anyhow::anyhow!(
            "Only file sources are supported in file processing"
        )),
    }
}
