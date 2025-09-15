use crate::core::{
    config::{adapter::ColumnConfig, project::RemoteDatabaseConfig},
    ducklake::DuckLake,
};
use anyhow::{Context, Result};
use std::sync::Arc;

use super::{ColumnInfo, DatabaseAdapter};

pub struct PostgresqlAdapter {
    ducklake: Arc<DuckLake>,
    config: RemoteDatabaseConfig,
}

impl PostgresqlAdapter {
    const ALIAS: &'static str = "postgres_db";
    pub fn new(ducklake: Arc<DuckLake>, config: RemoteDatabaseConfig) -> Self {
        Self { ducklake, config }
    }
}

impl DatabaseAdapter for PostgresqlAdapter {
    fn attach(&self) -> Result<()> {
        self.ducklake
            .execute_batch("INSTALL postgres; LOAD postgres;")
            .with_context(|| "Failed to install/load PostgreSQL extension")?;

        let password = self.config.password.plaintext()?;
        let connection_params = format!(
            "host={} port={} dbname={} user={} password={}",
            self.config.host,
            self.config.port,
            self.config.database,
            self.config.username,
            password
        );
        let attach_query = format!(
            "ATTACH '{}' AS {} (TYPE postgres);",
            connection_params,
            Self::ALIAS
        );

        self.ducklake
            .execute_batch(&attach_query)
            .with_context(|| {
                format!("Failed to attach PostgreSQL database. Query: {attach_query}")
            })?;
        Ok(())
    }

    fn detach(&self) -> Result<()> {
        let detach_query = format!("DETACH {}", Self::ALIAS);
        self.ducklake
            .execute_batch(&detach_query)
            .with_context(|| format!("Failed to detach PostgreSQL database: {}", Self::ALIAS))?;
        Ok(())
    }

    fn table_exists(&self, table: &str) -> Result<bool> {
        let validation_query = format!(
            "SELECT table_name FROM information_schema.tables WHERE table_name = '{table}'"
        );
        let validation_result = self
            .ducklake
            .query(&validation_query)
            .with_context(|| format!("Failed to validate table existence for: {table}"))?;

        let table_exists = !validation_result.is_empty()
            && !validation_result[0].is_empty()
            && validation_result[0][0] != "0";

        Ok(table_exists)
    }

    fn import_table(&self, source_table: &str, target_table: &str) -> Result<()> {
        let query = format!("SELECT * FROM {}.{}", Self::ALIAS, source_table);
        self.ducklake
            .create_table_from_query(target_table, &query)?;
        Ok(())
    }

    fn get_table_schema(&self, table: &str) -> Result<Vec<ColumnInfo>> {
        let schema_query = format!(
            "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{}' ORDER BY ordinal_position",
            table
        );

        let result = self
            .ducklake
            .query(&schema_query)
            .with_context(|| format!("Failed to get schema for table: {}", table))?;

        if result.is_empty() {
            return Err(anyhow::anyhow!(
                "Table '{}' does not exist or has no columns",
                table
            ));
        }

        let columns = result
            .into_iter()
            .map(|row| ColumnInfo {
                name: row[0].clone(),
                data_type: row[1].clone(),
            })
            .collect();

        Ok(columns)
    }

    fn validate_schema(&self, table: &str, expected_columns: &[ColumnConfig]) -> Result<()> {
        let actual_columns = self.get_table_schema(table)?;

        for expected in expected_columns {
            let found = actual_columns.iter().find(|col| col.name == expected.name);

            match found {
                None => {
                    return Err(anyhow::anyhow!(
                        "Column '{}' not found in table '{}'",
                        expected.name,
                        table
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

impl PostgresqlAdapter {
    fn types_match(&self, expected: &str, actual: &str) -> bool {
        let normalize_type = |t: &str| -> String {
            t.to_uppercase()
                .replace("INTEGER", "INT")
                .replace("STRING", "VARCHAR")
                .replace("CHARACTER VARYING", "VARCHAR")
                .split('(')
                .next()
                .unwrap_or(t)
                .to_string()
        };

        normalize_type(expected) == normalize_type(actual)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::adapter::test_helpers::{
        setup_postgres_test_data, setup_test_ducklake, test_encrypted_field,
    };
    use anyhow::Result;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_postgresql_adapter_basic_import() -> Result<()> {
        let tempdir = tempdir().unwrap();
        let ducklake = setup_test_ducklake(tempdir.path()).await.unwrap();

        setup_postgres_test_data().await.unwrap();

        let adapter = PostgresqlAdapter::new(
            ducklake.clone(),
            RemoteDatabaseConfig {
                host: "localhost".to_string(),
                port: 5433,
                database: "datasource_test".to_string(),
                username: "datasource".to_string(),
                password: test_encrypted_field("datasourcepass"),
            },
        );

        adapter.attach()?;

        let exists = adapter.table_exists("test_table")?;
        assert!(exists);

        let not_exists = adapter.table_exists("nonexistent_table")?;
        assert!(!not_exists);

        adapter.import_table("test_table", "imported_table")?;

        let result = ducklake.query("SELECT COUNT(*) FROM imported_table")?;
        assert_eq!(result[0][0], "3");

        let result = ducklake.query("SELECT name FROM imported_table ORDER BY id")?;
        assert_eq!(result[0][0], "Alice");
        assert_eq!(result[1][0], "Bob");
        assert_eq!(result[2][0], "Charlie");

        adapter.detach().unwrap();

        Ok(())
    }
}
