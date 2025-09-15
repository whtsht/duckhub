use crate::core::{config::model::ModelConfig, ducklake::DuckLake};
use anyhow::{Context, Result};
use std::sync::Arc;

#[derive(Clone)]
pub struct Model {
    config: ModelConfig,
    ducklake: Arc<DuckLake>,
}

impl Model {
    pub fn new(config: ModelConfig, ducklake: Arc<DuckLake>) -> Self {
        Self { config, ducklake }
    }

    pub async fn transform(&self, table_name: &str) -> Result<()> {
        let create_table_sql = format!(
            "CREATE OR REPLACE TABLE {} AS ({});",
            table_name, self.config.sql
        );

        self.ducklake
            .execute_batch(&create_table_sql)
            .with_context(|| {
                format!("Failed to execute model transformation. SQL: {create_table_sql}")
            })?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::core::config::project::StorageConfig;

    use super::*;

    fn create_test_model_config() -> ModelConfig {
        ModelConfig {
            sql: "SELECT * FROM test_table".to_string(),
            description: None,
        }
    }

    #[tokio::test]
    async fn test_model_creation() {
        use crate::core::ducklake::CatalogConfig;

        let config = create_test_model_config();

        let catalog_config = CatalogConfig::Sqlite {
            path: "/tmp/test_catalog.sqlite".to_string(),
        };
        let storage_config = StorageConfig::LocalFile {
            path: "/tmp/test_storage".to_string(),
        };

        let ducklake = Arc::new(DuckLake::new(catalog_config, storage_config).await.unwrap());
        let model = Model::new(config, ducklake);
        assert_eq!(model.config.sql, "SELECT * FROM test_table");
    }
}
