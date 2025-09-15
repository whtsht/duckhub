use super::{Adapter, adapter_from_connection};
use crate::core::config::{
    adapter::{AdapterSource, ColumnConfig},
    project::ConnectionConfig,
};
use anyhow::Result;
use mysql::MysqlAdapter;
use postgresql::PostgresqlAdapter;
use serde::{Deserialize, Serialize};
use sqlite::SqliteAdapter;
use std::collections::HashMap;

pub mod mysql;
pub mod postgresql;
pub mod sqlite;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnInfo {
    pub name: String,
    pub data_type: String,
}

pub trait DatabaseAdapter: Send + Sync {
    fn attach(&self) -> Result<()>;
    fn detach(&self) -> Result<()>;
    fn table_exists(&self, table: &str) -> Result<bool>;
    fn import_table(&self, source_table: &str, target_table: &str) -> Result<()>;
    fn get_table_schema(&self, table: &str) -> Result<Vec<ColumnInfo>>;
    fn validate_schema(&self, table: &str, expected_columns: &[ColumnConfig]) -> Result<()>;
}

impl Adapter {
    pub fn database_adapter(
        &self,
        name: &str,
        connections: &HashMap<String, ConnectionConfig>,
    ) -> Result<Box<dyn DatabaseAdapter>> {
        let connection = adapter_from_connection(name, connections)?;

        match connection {
            ConnectionConfig::Sqlite { path } => {
                let adapter = SqliteAdapter::new(self.ducklake.clone(), path);
                Ok(Box::new(adapter))
            }
            ConnectionConfig::MySql(config) => {
                let adapter = MysqlAdapter::new(self.ducklake.clone(), config);
                Ok(Box::new(adapter))
            }
            ConnectionConfig::PostgreSql(config) => {
                let adapter = PostgresqlAdapter::new(self.ducklake.clone(), config);
                Ok(Box::new(adapter))
            }
            _ => Err(anyhow::anyhow!(
                "Unsupported connection type for database adapter"
            )),
        }
    }

    pub async fn database_import(
        &self,
        table_name: &str,
        adapter: Box<dyn DatabaseAdapter>,
    ) -> Result<()> {
        adapter.attach()?;
        let source_table = if let AdapterSource::Database { table_name, .. } = &self.config.source {
            table_name
        } else {
            return Err(anyhow::anyhow!("Adapter source is not a database"));
        };
        if adapter.table_exists(source_table)? {
            adapter.import_table(source_table, table_name)?;
            adapter.detach()?;
            Ok(())
        } else {
            adapter.detach()?;
            Err(anyhow::anyhow!(
                "Source table '{}' does not exist in the database",
                source_table
            ))
        }
    }
}
