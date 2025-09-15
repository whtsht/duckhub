use crate::core::{config::adapter::ColumnConfig, ducklake::DuckLake};
use anyhow::{Context, Result};
use std::sync::Arc;

use super::{ColumnInfo, DatabaseAdapter};

pub struct SqliteAdapter {
    ducklake: Arc<DuckLake>,
    path: String,
}

impl SqliteAdapter {
    pub fn new(ducklake: Arc<DuckLake>, path: String) -> Self {
        Self { ducklake, path }
    }
}

impl DatabaseAdapter for SqliteAdapter {
    fn attach(&self) -> Result<()> {
        let attach_query = "INSTALL sqlite_scanner; LOAD sqlite_scanner;";
        self.ducklake
            .execute_batch(attach_query)
            .with_context(|| "Failed to install sqlite_scanner".to_string())?;
        Ok(())
    }

    fn detach(&self) -> Result<()> {
        Ok(())
    }

    fn table_exists(&self, table: &str) -> Result<bool> {
        let validation_query = format!(
            "SELECT COUNT(*) as count FROM sqlite_scan('{}', '{}')",
            self.path, table
        );

        match self.ducklake.query(&validation_query) {
            Ok(validation_result) => {
                let table_exists = !validation_result.is_empty()
                    && !validation_result[0].is_empty()
                    && validation_result[0][0] != "0";
                Ok(table_exists)
            }
            Err(_) => Ok(false),
        }
    }

    fn import_table(&self, source_table: &str, target_table: &str) -> Result<()> {
        let query = format!(
            "SELECT * FROM sqlite_scan('{}', '{}')",
            self.path, source_table
        );
        self.ducklake
            .create_table_from_query(target_table, &query)?;
        Ok(())
    }

    fn get_table_schema(&self, table: &str) -> Result<Vec<ColumnInfo>> {
        let pragma_query = format!("PRAGMA table_info({})", table);

        let conn = rusqlite::Connection::open(&self.path)
            .with_context(|| format!("Failed to open SQLite database: {}", self.path))?;

        let mut stmt = conn
            .prepare(&pragma_query)
            .with_context(|| format!("Failed to prepare PRAGMA query for table: {}", table))?;

        let column_info_iter = stmt
            .query_map([], |row| {
                Ok(ColumnInfo {
                    name: row.get::<_, String>(1)?,
                    data_type: row.get::<_, String>(2)?,
                })
            })
            .with_context(|| format!("Failed to execute PRAGMA query for table: {}", table))?;

        let mut columns = Vec::new();
        for column_info in column_info_iter {
            columns.push(column_info.with_context(|| "Failed to parse column info")?);
        }

        if columns.is_empty() {
            return Err(anyhow::anyhow!(
                "Table '{}' does not exist or has no columns",
                table
            ));
        }

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

impl SqliteAdapter {
    fn types_match(&self, expected: &str, actual: &str) -> bool {
        let normalize_type = |t: &str| -> String {
            t.to_uppercase()
                .replace("VARCHAR", "TEXT")
                .replace("CHAR", "TEXT")
                .replace("STRING", "TEXT")
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
    use crate::core::adapter::test_helpers::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_sqlite_adapter_basic_import() {
        let tempdir = tempdir().unwrap();
        let ducklake = setup_test_ducklake(tempdir.path()).await.unwrap();

        let db_path = tempdir.path().join("test.db");
        create_test_sqlite_db(&db_path).await.unwrap();

        let adapter = SqliteAdapter::new(ducklake.clone(), db_path.to_string_lossy().to_string());

        adapter.attach().unwrap();

        let exists = adapter.table_exists("test_table").unwrap();
        assert!(exists);

        let not_exists = adapter.table_exists("nonexistent_table").unwrap();
        assert!(!not_exists);

        adapter
            .import_table("test_table", "imported_table")
            .unwrap();

        let result = ducklake
            .query("SELECT COUNT(*) FROM imported_table")
            .unwrap();
        assert_eq!(result[0][0], "3");

        let result = ducklake
            .query("SELECT name FROM imported_table ORDER BY id")
            .unwrap();
        assert_eq!(result[0][0], "Alice");
        assert_eq!(result[1][0], "Bob");
        assert_eq!(result[2][0], "Charlie");

        adapter.detach().unwrap();
    }

    #[tokio::test]
    async fn test_sqlite_adapter_nonexistent_table() {
        let tempdir = tempdir().unwrap();
        let ducklake = setup_test_ducklake(tempdir.path()).await.unwrap();

        let db_path = tempdir.path().join("test.db");
        create_test_sqlite_db(&db_path).await.unwrap();

        let adapter = SqliteAdapter::new(ducklake.clone(), db_path.to_string_lossy().to_string());

        adapter.attach().unwrap();

        let exists = adapter.table_exists("nonexistent_table").unwrap();
        assert!(!exists);

        let result = adapter.import_table("nonexistent_table", "imported_table");
        assert!(result.is_err());
    }
}
