use crate::{api::Error, core::graph::Graph};
use axum::{Extension, Json, Router, routing::get};
use std::sync::Arc;
use tokio::sync::Mutex;

pub fn routes() -> Router {
    Router::new().route("/graph", get(get_graph))
}

async fn get_graph(Extension(graph): Extension<Arc<Mutex<Graph>>>) -> Result<Json<Graph>, Error> {
    let graph = graph.lock().await;
    Ok(Json(graph.clone()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_helpers::TestManager;
    use anyhow::Result;
    use serde_json::json;

    #[tokio::test]
    async fn test_graph_updates_after_adapter_creation() -> Result<()> {
        let test = TestManager::new();
        let adapter_routes = crate::api::adapter::routes();
        let graph_routes = routes();

        let server =
            test.setup_server(move || Router::new().merge(adapter_routes).merge(graph_routes));

        // Initially graph should be empty
        let graph_response = server.get("/graph").await;
        graph_response.assert_status_ok();
        let graph: Graph = graph_response.json();
        assert_eq!(graph.nodes.len(), 0);

        // Create an adapter
        let new_adapter = json!({
            "name": "test_adapter",
            "config": {
                "description": "Test adapter",
                "connection": "test_connection",
                "source": {
                    "type": "database",
                    "table_name": "test_table"
                },
                "columns": []
            }
        });

        let create_response = server.post("/adapters").json(&new_adapter).await;
        create_response.assert_status_ok();

        // Check that graph now contains the adapter
        let graph_response = server.get("/graph").await;
        graph_response.assert_status_ok();
        let graph: Graph = graph_response.json();
        assert_eq!(graph.nodes.len(), 1);
        assert!(graph.nodes.contains_key("test_adapter"));

        Ok(())
    }

    #[tokio::test]
    async fn test_graph_updates_after_model_creation() -> Result<()> {
        let test = TestManager::new();
        let adapter_routes = crate::api::adapter::routes();
        let model_routes = crate::api::model::routes();
        let graph_routes = routes();

        let server = test.setup_server(move || {
            Router::new()
                .merge(adapter_routes)
                .merge(model_routes)
                .merge(graph_routes)
        });

        // Create an adapter first
        let new_adapter = json!({
            "name": "users",
            "config": {
                "description": "Users table",
                "connection": "test_connection",
                "source": {
                    "type": "database",
                    "table_name": "users"
                },
                "columns": []
            }
        });

        server
            .post("/adapters")
            .json(&new_adapter)
            .await
            .assert_status_ok();

        // Create a model that depends on the adapter
        let new_model = json!({
            "name": "active_users",
            "config": {
                "description": "Active users model",
                "sql": "SELECT * FROM users WHERE active = true"
            }
        });

        server
            .post("/models")
            .json(&new_model)
            .await
            .assert_status_ok();

        // Check that graph contains both nodes with correct dependencies
        let graph_response = server.get("/graph").await;
        graph_response.assert_status_ok();
        let graph: Graph = graph_response.json();

        assert_eq!(graph.nodes.len(), 2);
        assert!(graph.nodes.contains_key("users"));
        assert!(graph.nodes.contains_key("active_users"));

        let active_users_node = graph.nodes.get("active_users").unwrap();
        assert_eq!(active_users_node.dependencies, vec!["users"]);

        Ok(())
    }
}
