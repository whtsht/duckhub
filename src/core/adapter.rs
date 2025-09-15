pub mod database;
pub mod file;

#[cfg(test)]
pub mod test_helpers;

use crate::core::{
    config::{
        adapter::{AdapterConfig, AdapterSource},
        project::ConnectionConfig,
    },
    ducklake::DuckLake,
};
use anyhow::Result;
use std::{collections::HashMap, sync::Arc};

#[derive(Clone)]
pub struct Adapter {
    config: AdapterConfig,
    ducklake: Arc<DuckLake>,
}

impl Adapter {
    pub fn new(config: AdapterConfig, ducklake: Arc<DuckLake>) -> Self {
        Self { config, ducklake }
    }
}

impl Adapter {
    pub async fn validate_schema(
        &self,
        connections: &HashMap<String, ConnectionConfig>,
    ) -> Result<()> {
        match &self.config.source {
            AdapterSource::Database { table_name } => {
                let database_adapter =
                    self.database_adapter(&self.config.connection, connections)?;
                database_adapter.validate_schema(table_name, &self.config.columns)
            }
            AdapterSource::File { file, .. } => {
                let file_adapter = self.file_adapter(&self.config.connection, connections)?;
                let file_paths = file_adapter.list_files(&file.path).await?;
                if file_paths.is_empty() {
                    return Err(anyhow::anyhow!(
                        "No files found matching pattern: {}",
                        file.path
                    ));
                }
                file_adapter
                    .validate_schema(&file_paths[0], &self.config.columns)
                    .await
            }
        }
    }

    pub async fn get_schema(
        &self,
        connections: &HashMap<String, ConnectionConfig>,
    ) -> Result<Vec<database::ColumnInfo>> {
        match &self.config.source {
            AdapterSource::Database { table_name } => {
                let database_adapter =
                    self.database_adapter(&self.config.connection, connections)?;
                database_adapter.get_table_schema(table_name)
            }
            AdapterSource::File { file, .. } => {
                let file_adapter = self.file_adapter(&self.config.connection, connections)?;
                let file_paths = file_adapter.list_files(&file.path).await?;
                if file_paths.is_empty() {
                    return Err(anyhow::anyhow!(
                        "No files found matching pattern: {}",
                        file.path
                    ));
                }
                file_adapter.get_file_schema(&file_paths[0]).await
            }
        }
    }

    pub async fn import(
        &self,
        table_name: &str,
        connections: &HashMap<String, ConnectionConfig>,
    ) -> Result<()> {
        match &self.config.source {
            AdapterSource::File { .. } => {
                let adapter = self.file_adapter(&self.config.connection, connections)?;
                self.file_import(table_name, adapter).await
            }
            AdapterSource::Database { .. } => {
                let adapter = self.database_adapter(&self.config.connection, connections)?;
                self.database_import(table_name, adapter).await
            }
        }
    }
}

pub fn adapter_from_connection(
    name: &str,
    connections: &HashMap<String, ConnectionConfig>,
) -> Result<ConnectionConfig> {
    if let Some(connection) = connections.get(name) {
        Ok(connection.clone())
    } else {
        Err(anyhow::anyhow!("Connection '{}' not found", name))
    }
}
