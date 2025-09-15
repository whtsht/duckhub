use crate::{
    api::Error,
    core::{
        adapter::Adapter,
        config::{
            Config,
            adapter::{AdapterConfig, AdapterSource, ColumnConfig},
        },
        graph::Graph,
    },
};
use anyhow::Result;
use axum::{
    Extension, Router,
    extract::Path,
    http::StatusCode,
    response::Json,
    routing::{get, post},
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Serialize, Deserialize)]
pub struct AdapterSummary {
    pub name: String,
    pub description: Option<String>,
    pub connection: String,
    pub source_type: String,
}

#[derive(Deserialize)]
pub struct CreateAdapterRequest {
    pub name: String,
    pub config: AdapterConfig,
}

#[derive(Deserialize)]
pub struct TestSchemaRequest {
    pub connection: String,
    pub source: AdapterSource,
    pub columns: Vec<ColumnConfig>,
}

#[derive(Deserialize)]
pub struct GetSchemaRequest {
    pub connection: String,
    pub source: AdapterSource,
}

pub fn routes() -> Router {
    Router::new()
        .route("/adapters", get(list_adapters).post(create_adapter))
        .route(
            "/adapters/{name}",
            get(get_adapter).put(update_adapter).delete(delete_adapter),
        )
        .route("/adapters/test-schema", post(test_schema))
        .route("/adapters/get-schema", post(get_schema))
}

async fn list_adapters(
    Extension(config): Extension<Arc<Mutex<Config>>>,
) -> Result<Json<Vec<AdapterSummary>>, Error> {
    let mut adapters: Vec<AdapterSummary> = config
        .lock()
        .await
        .adapters
        .clone()
        .into_iter()
        .map(|(name, config)| {
            let source_type = match &config.source {
                AdapterSource::File { .. } => "file".to_string(),
                AdapterSource::Database { .. } => "database".to_string(),
            };
            AdapterSummary {
                name,
                description: config.description,
                connection: config.connection,
                source_type,
            }
        })
        .collect();

    adapters.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(Json(adapters))
}

async fn get_adapter(
    Extension(config): Extension<Arc<Mutex<Config>>>,
    Path(name): Path<String>,
) -> Result<Json<AdapterConfig>, Error> {
    if let Some(adapter_config) = config.lock().await.adapters.get(&name) {
        Ok(Json(adapter_config.clone()))
    } else {
        Error::not_found().build()
    }
}

async fn create_adapter(
    Extension(config): Extension<Arc<Mutex<Config>>>,
    Extension(graph): Extension<Arc<Mutex<Graph>>>,
    Json(adapter): Json<CreateAdapterRequest>,
) -> Result<(), Error> {
    let mut config = config.lock().await;

    if config.adapters.contains_key(&adapter.name) {
        return Error::conflict().build();
    }

    let mut graph = graph.lock().await;
    graph.create_node(&adapter.name, &[]);
    graph.save().await?;

    let adapter_file = config.upsert_adapter(&adapter.name, &adapter.config)?;
    adapter_file.save()?;

    Ok(())
}

async fn update_adapter(
    Extension(config): Extension<Arc<Mutex<Config>>>,
    Extension(graph): Extension<Arc<Mutex<Graph>>>,
    Path(name): Path<String>,
    Json(adapter): Json<AdapterConfig>,
) -> Result<(), Error> {
    let mut config = config.lock().await;

    if !config.adapters.contains_key(&name) {
        return Error::not_found().build();
    };

    let mut graph = graph.lock().await;
    graph.update_node(&name);
    graph.save().await?;

    let adapter_file = config.upsert_adapter(&name, &adapter)?;
    adapter_file.save()?;

    Ok(())
}

async fn delete_adapter(
    Extension(config): Extension<Arc<Mutex<Config>>>,
    Extension(graph): Extension<Arc<Mutex<Graph>>>,
    Path(name): Path<String>,
) -> Result<StatusCode, Error> {
    let mut config = config.lock().await;

    if !config.adapters.contains_key(&name) {
        return Error::not_found().build();
    };

    let mut graph = graph.lock().await;
    graph.delete_node(&name);
    graph.save().await?;

    let adapter_file = config.delete_adapter(&name)?;
    adapter_file.save()?;

    Ok(StatusCode::NO_CONTENT)
}

async fn test_schema(
    Extension(config): Extension<Arc<Mutex<Config>>>,
    Json(request): Json<TestSchemaRequest>,
) -> Result<(), Error> {
    let config = config.lock().await;

    if !config.project.connections.contains_key(&request.connection) {
        return Err(Error::bad_request()
            .with_message(format!("Connection '{}' not found", request.connection)));
    }

    let test_adapter_config = AdapterConfig {
        connection: request.connection.clone(),
        description: Some("Test schema validation".to_string()),
        source: request.source.clone(),
        columns: request.columns.clone(),
    };

    let ducklake = Arc::new(
        crate::core::ducklake::DuckLake::from_config(&config)
            .await
            .map_err(|e| {
                Error::internal_server_error()
                    .with_message(format!("Failed to initialize DuckLake: {}", e))
            })?,
    );
    let test_adapter = Adapter::new(test_adapter_config, ducklake);

    test_adapter
        .validate_schema(&config.project.connections)
        .await
        .map_err(|e| Error::bad_request().with_message(e.to_string()))?;

    Ok(())
}

async fn get_schema(
    Extension(config): Extension<Arc<Mutex<Config>>>,
    Json(request): Json<GetSchemaRequest>,
) -> Result<Json<Vec<crate::core::adapter::database::ColumnInfo>>, Error> {
    let config = config.lock().await;

    if !config.project.connections.contains_key(&request.connection) {
        return Err(Error::bad_request()
            .with_message(format!("Connection '{}' not found", request.connection)));
    }

    let test_adapter_config = AdapterConfig {
        connection: request.connection.clone(),
        description: Some("Get schema".to_string()),
        source: request.source.clone(),
        columns: vec![],
    };

    let ducklake = Arc::new(
        crate::core::ducklake::DuckLake::from_config(&config)
            .await
            .map_err(|e| {
                Error::internal_server_error()
                    .with_message(format!("Failed to initialize DuckLake: {}", e))
            })?,
    );
    let test_adapter = Adapter::new(test_adapter_config, ducklake);

    let schema = test_adapter
        .get_schema(&config.project.connections)
        .await
        .map_err(|e| Error::bad_request().with_message(e.to_string()))?;

    Ok(Json(schema))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        core::{
            adapter::test_helpers::write_test_file,
            config::{
                adapter::{FileConfig, FormatConfig},
                project::ConnectionConfig,
            },
        },
        test_helpers::TestManager,
    };
    use anyhow::Result;
    use serde_json::json;

    #[tokio::test]
    async fn test_create_adapter() -> Result<()> {
        let test = TestManager::new();
        let server = test.setup_server(routes);

        // Create request
        let new_adapter = json!({
            "name": "test_csv_adapter",
            "config": {
                "description": "Test CSV adapter",
                "connection": "test_connection",
                "source": {
                    "type": "file",
                    "file": {
                        "path": "test.csv"
                    },
                    "format": {
                        "type": "csv",
                        "has_header": true,
                        "delimiter": ","
                    }
                },
                "columns": []
            }
        });

        let response = server.post("/adapters").json(&new_adapter).await;
        response.assert_status_ok();

        let graph = Graph::load(test.directory()).await?;
        assert!(graph.has_node("test_csv_adapter"));

        // Get request
        let response = server.get("/adapters/test_csv_adapter").await;
        response.assert_status_ok();

        let adapter_config: AdapterConfig = response.json();
        assert_eq!(
            adapter_config.description,
            Some("Test CSV adapter".to_string())
        );
        assert_eq!(adapter_config.connection, "test_connection");
        match &adapter_config.source {
            AdapterSource::File { file, format } => {
                assert_eq!(file.path, "test.csv");
                assert_eq!(format.ty, "csv");
                assert_eq!(format.has_header, Some(true));
                assert_eq!(format.delimiter, Some(",".to_string()));
            }
            _ => panic!("Expected File source"),
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_update_adapter() -> Result<()> {
        let test = TestManager::new();
        let server = test.setup_server(routes);

        let original_adapter = AdapterConfig {
            connection: "test_connection".to_string(),
            description: Some("Original adapter".to_string()),
            source: AdapterSource::File {
                file: FileConfig {
                    path: "original.csv".to_string(),
                    compression: None,
                    max_batch_size: None,
                },
                format: FormatConfig {
                    ty: "csv".to_string(),
                    delimiter: Some(",".to_string()),
                    null_value: None,
                    has_header: Some(true),
                },
            },
            columns: vec![],
        };

        // Create adapter directly
        {
            let mut config = test.config().await;
            let adapter_file = config.upsert_adapter("test_adapter", &original_adapter)?;
            adapter_file.save()?;
        }

        // Create node in graph
        {
            let mut graph = test.graph().await;
            graph.create_node("test_adapter", &[]);
            graph.set_current_time("test_adapter");
        }

        // Update request
        let updated_config = json!({
            "description": "Updated adapter",
            "connection": "test_connection",
            "source": {
                "type": "file",
                "file": {
                    "path": "updated.csv"
                },
                "format": {
                    "type": "csv",
                    "has_header": false,
                    "delimiter": ";"
                }
            },
            "columns": []
        });

        let response = server
            .put("/adapters/test_adapter")
            .json(&updated_config)
            .await;
        response.assert_status_ok();

        // GET request to verify update
        let get_response = server.get("/adapters/test_adapter").await;
        get_response.assert_status_ok();

        let adapter_config: AdapterConfig = get_response.json();
        assert_eq!(
            adapter_config.description,
            Some("Updated adapter".to_string())
        );
        match &adapter_config.source {
            AdapterSource::File { file, format } => {
                assert_eq!(file.path, "updated.csv");
                assert_eq!(format.has_header, Some(false));
                assert_eq!(format.delimiter, Some(";".to_string()));
            }
            _ => panic!("Expected File source"),
        }

        // Verify graph node was updated (timestamp reset)
        {
            let graph = test.graph().await;
            assert!(graph.has_node("test_adapter"));
            assert!(graph.get_node("test_adapter").unwrap().updated_at.is_none());
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_get_schema() -> Result<()> {
        let test = TestManager::new();
        let server = test.setup_server(routes);

        write_test_file(
            test.directory(),
            "test_data.csv",
            "id,name,age
1,Alice,25
2,Bob,30",
        )
        .unwrap();

        {
            let mut config = test.config().await;
            let mut project_config = config.project.clone();
            project_config.connections.insert(
                "test_connection".to_string(),
                ConnectionConfig::LocalFile {
                    base_path: test.directory().to_string_lossy().to_string(),
                },
            );
            let project_file = config.add_project_setting(&project_config)?;
            project_file.save()?;
        }

        let adapter_config = AdapterConfig {
            connection: "test_connection".to_string(),
            description: Some("Test adapter".to_string()),
            source: AdapterSource::File {
                file: FileConfig {
                    path: "test_data.csv".to_string(),
                    compression: None,
                    max_batch_size: None,
                },
                format: FormatConfig {
                    ty: "csv".to_string(),
                    delimiter: Some(",".to_string()),
                    null_value: None,
                    has_header: Some(true),
                },
            },
            columns: vec![],
        };

        let get_schema_request = json!({
            "connection": "test_connection",
            "source": adapter_config.source
        });

        let response = server
            .post("/adapters/get-schema")
            .json(&get_schema_request)
            .await;
        response.assert_status_ok();

        let schema: Vec<crate::core::adapter::database::ColumnInfo> = response.json();
        assert_eq!(schema.len(), 3);
        assert_eq!(schema[0].name, "id");
        assert_eq!(schema[1].name, "name");
        assert_eq!(schema[2].name, "age");

        Ok(())
    }

    #[tokio::test]
    async fn test_delete_adapter() -> Result<()> {
        let test = TestManager::new();
        let server = test.setup_server(routes);

        let adapter_config = AdapterConfig {
            connection: "test_connection".to_string(),
            description: Some("Adapter to delete".to_string()),
            source: AdapterSource::Database {
                table_name: "test_table".to_string(),
            },
            columns: vec![],
        };

        {
            let mut config = test.config().await;
            let adapter_file = config.upsert_adapter("adapter_to_delete", &adapter_config)?;
            adapter_file.save()?;
        }

        let mut graph = Graph::load(test.directory()).await?;
        graph.create_node("adapter_to_delete", &[]);
        graph.save().await?;

        let get_response = server.get("/adapters/adapter_to_delete").await;
        get_response.assert_status_ok();

        let delete_response = server.delete("/adapters/adapter_to_delete").await;
        delete_response.assert_status(StatusCode::NO_CONTENT);

        let get_response_after = server.get("/adapters/adapter_to_delete").await;
        get_response_after.assert_status(StatusCode::NOT_FOUND);

        let graph = Graph::load(test.directory()).await?;
        assert!(!graph.has_node("adapter_to_delete"));

        Ok(())
    }
}
