use crate::core::{
    config::adapter::{AdapterConfig, ColumnConfig},
    ducklake::DuckLake,
};
use anyhow::{Context, Result};
use async_trait::async_trait;
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use super::{super::database::ColumnInfo, FileAdapter, build_import_query};

pub struct LocalFileAdapter {
    ducklake: Arc<DuckLake>,
    adapter_config: AdapterConfig,
    base_path: Option<String>,
}

impl LocalFileAdapter {
    pub fn new(
        ducklake: Arc<DuckLake>,
        adapter_config: AdapterConfig,
        base_path: Option<String>,
    ) -> Self {
        Self {
            ducklake,
            adapter_config,
            base_path,
        }
    }
}

#[async_trait]
impl FileAdapter for LocalFileAdapter {
    async fn list_files(&self, path: &str) -> Result<Vec<String>> {
        let resolved_pattern = if let Some(base) = &self.base_path {
            if path.starts_with('/') {
                path.to_string()
            } else {
                PathBuf::from(base).join(path).to_string_lossy().to_string()
            }
        } else {
            path.to_string()
        };

        let mut existing_paths = Vec::new();
        if resolved_pattern.contains('*') || resolved_pattern.contains('?') {
            let glob_matches: Vec<_> = glob::glob(&resolved_pattern)
                .context("Failed to execute glob pattern")?
                .filter_map(Result::ok)
                .map(|p| p.to_string_lossy().to_string())
                .collect();
            existing_paths.extend(glob_matches);
        } else if Path::new(&resolved_pattern).exists() {
            existing_paths.push(resolved_pattern);
        }

        Ok(existing_paths)
    }

    async fn import_files(&self, table_name: &str, files: &[String]) -> Result<()> {
        if files.is_empty() {
            return Ok(());
        }

        let query = build_import_query(&self.adapter_config, files)?;
        self.ducklake.create_table_from_query(table_name, &query)?;
        Ok(())
    }

    async fn get_file_schema(&self, file_path: &str) -> Result<Vec<ColumnInfo>> {
        let resolved_path = if let Some(base) = &self.base_path {
            if file_path.starts_with('/') {
                file_path.to_string()
            } else {
                PathBuf::from(base)
                    .join(file_path)
                    .to_string_lossy()
                    .to_string()
            }
        } else {
            file_path.to_string()
        };

        if !Path::new(&resolved_path).exists() {
            return Err(anyhow::anyhow!("File '{}' does not exist", resolved_path));
        }

        let temp_table = format!("temp_schema_check_{}", uuid::Uuid::new_v4().simple());

        match &self.adapter_config.source {
            crate::core::config::adapter::AdapterSource::File { format, .. } => {
                let query = match format.ty.as_str() {
                    "csv" => {
                        let has_header = format.has_header.unwrap_or(true);
                        format!(
                            "CREATE TEMP TABLE {temp_table} AS SELECT * FROM read_csv_auto('{resolved_path}', header={has_header}) LIMIT 0"
                        )
                    }
                    "json" => {
                        format!(
                            "CREATE TEMP TABLE {temp_table} AS SELECT * FROM read_json_auto('{resolved_path}') LIMIT 0"
                        )
                    }
                    "parquet" => {
                        format!(
                            "CREATE TEMP TABLE {temp_table} AS SELECT * FROM read_parquet('{resolved_path}') LIMIT 0"
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

impl LocalFileAdapter {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{
        adapter::test_helpers::*,
        config::adapter::{AdapterSource, FileConfig},
    };
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_localfile_adapter_csv_import() {
        let tempdir = tempdir().unwrap();
        let ducklake = setup_test_ducklake(tempdir.path()).await.unwrap();

        write_test_file(tempdir.path(), "test_data.csv", &create_test_csv_data()).unwrap();

        let adapter_config = create_csv_adapter_config();
        let adapter = LocalFileAdapter::new(
            ducklake.clone(),
            adapter_config,
            Some(tempdir.path().to_string_lossy().to_string()),
        );

        let file_config = FileConfig {
            path: "test_data.csv".to_string(),
            compression: None,
            max_batch_size: None,
        };

        let files = adapter.list_files(&file_config.path).await.unwrap();
        assert_eq!(files.len(), 1);
        assert!(files[0].ends_with("test_data.csv"));

        adapter.import_files("test_table", &files).await.unwrap();

        let result = ducklake.query("SELECT COUNT(*) FROM test_table").unwrap();
        assert_eq!(result[0][0], "3");

        let result = ducklake
            .query("SELECT name FROM test_table ORDER BY id")
            .unwrap();
        assert_eq!(result[0][0], "Alice");
        assert_eq!(result[1][0], "Bob");
        assert_eq!(result[2][0], "Charlie");
    }

    #[tokio::test]
    async fn test_localfile_adapter_json_import() {
        let tempdir = tempdir().unwrap();
        let ducklake = setup_test_ducklake(tempdir.path()).await.unwrap();

        write_test_file(tempdir.path(), "test_data.json", &create_test_json_data()).unwrap();

        let adapter_config = create_json_adapter_config();
        let adapter = LocalFileAdapter::new(
            ducklake.clone(),
            adapter_config,
            Some(tempdir.path().to_string_lossy().to_string()),
        );

        let file_config = FileConfig {
            path: "test_data.json".to_string(),
            compression: None,
            max_batch_size: None,
        };

        let files = adapter.list_files(&file_config.path).await.unwrap();
        assert_eq!(files.len(), 1);

        adapter.import_files("test_table", &files).await.unwrap();

        let result = ducklake.query("SELECT COUNT(*) FROM test_table").unwrap();
        assert_eq!(result[0][0], "3");
    }

    #[tokio::test]
    async fn test_localfile_adapter_multiple_files() {
        let tempdir = tempdir().unwrap();
        let ducklake = setup_test_ducklake(tempdir.path()).await.unwrap();

        write_test_file(tempdir.path(), "file1.csv", "id,name\n1,Alice\n2,Bob").unwrap();
        write_test_file(tempdir.path(), "file2.csv", "id,name\n3,Charlie\n4,David").unwrap();

        let mut adapter_config = create_csv_adapter_config();
        if let AdapterSource::File { file, .. } = &mut adapter_config.source {
            file.path = "file*.csv".to_string();
        }

        let adapter = LocalFileAdapter::new(
            ducklake.clone(),
            adapter_config,
            Some(tempdir.path().to_string_lossy().to_string()),
        );

        let file_config = FileConfig {
            path: "file*.csv".to_string(),
            compression: None,
            max_batch_size: None,
        };

        let files = adapter.list_files(&file_config.path).await.unwrap();
        assert_eq!(files.len(), 2);

        adapter.import_files("test_table", &files).await.unwrap();

        let result = ducklake.query("SELECT COUNT(*) FROM test_table").unwrap();
        assert_eq!(result[0][0], "4");
    }

    #[tokio::test]
    async fn test_localfile_adapter_empty_files() {
        let tempdir = tempdir().unwrap();
        let ducklake = setup_test_ducklake(tempdir.path()).await.unwrap();

        let adapter_config = create_csv_adapter_config();
        let adapter = LocalFileAdapter::new(
            ducklake.clone(),
            adapter_config,
            Some(tempdir.path().to_string_lossy().to_string()),
        );

        let result = adapter.import_files("test_table", &[]).await;
        assert!(result.is_ok());
    }
}
