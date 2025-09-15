use crate::{
    api::Error,
    core::{
        config::{Config, query::QueryConfig},
        ducklake::DuckLake,
    },
};
use anyhow::Result;
use axum::{
    Extension, Router,
    extract::Path as AxumPath,
    response::Json,
    routing::{get, post},
};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, sync::Arc};
use tokio::sync::Mutex;

#[derive(Deserialize)]
pub struct QueryRequest {
    pub sql: String,
}

#[derive(Deserialize)]
pub struct CreateQueryRequest {
    pub name: String,
    pub config: QueryConfig,
}

#[derive(Serialize, Deserialize)]
pub struct QueryResult {
    pub data: std::collections::HashMap<String, Vec<String>>,
    pub row_count: usize,
    pub column_count: usize,
}

#[derive(Serialize, Deserialize)]
pub struct QuerySummary {
    pub name: String,
    pub description: Option<String>,
}

#[derive(Serialize, Deserialize)]
pub struct QueryListResponse {
    pub queries: HashMap<String, QueryConfig>,
}

pub fn routes() -> Router {
    Router::new()
        .route("/query", post(run_adhoc_query))
        .route("/queries", get(list_queries).post(create_query))
        .route(
            "/queries/{name}",
            get(get_query).put(update_query).delete(delete_query),
        )
        .route("/queries/{name}/run", post(run_query))
}

async fn run_adhoc_query(
    Extension(config): Extension<Arc<Mutex<Config>>>,
    Json(payload): Json<QueryRequest>,
) -> Result<Json<QueryResult>, Error> {
    let config = config.lock().await;
    let data = execute_query(&config, &payload.sql).await?;
    let row_count = if data.is_empty() {
        0
    } else {
        data.values().next().map(|v| v.len()).unwrap_or(0)
    };
    let column_count = data.len();

    Ok(Json(QueryResult {
        data,
        row_count,
        column_count,
    }))
}

pub async fn execute_query(
    config: &Config,
    sql: &str,
) -> Result<std::collections::HashMap<String, Vec<String>>, Error> {
    let ducklake = DuckLake::from_config(config).await?;
    let results = ducklake
        .query_with_column_names(sql)
        .map_err(|e| Error::bad_request().with_message(format!("{e}")))?;

    Ok(results)
}

async fn list_queries(
    Extension(config): Extension<Arc<Mutex<Config>>>,
) -> Result<Json<QueryListResponse>, Error> {
    let config = config.lock().await;
    Ok(Json(QueryListResponse {
        queries: config.queries.clone(),
    }))
}

async fn create_query(
    Extension(config): Extension<Arc<Mutex<Config>>>,
    Json(query): Json<CreateQueryRequest>,
) -> Result<(), Error> {
    let mut config = config.lock().await;
    if config.queries.contains_key(&query.name) {
        return Error::conflict().build();
    }

    let query_file = config.upsert_query(&query.name, &query.config)?;
    query_file.save()?;

    Ok(())
}

async fn get_query(
    Extension(config): Extension<Arc<Mutex<Config>>>,
    AxumPath(name): AxumPath<String>,
) -> Result<Json<QueryConfig>, Error> {
    let config = config.lock().await;
    match config.queries.get(&name) {
        Some(query) => Ok(Json(query.clone())),
        None => Error::not_found().build(),
    }
}

async fn update_query(
    Extension(config): Extension<Arc<Mutex<Config>>>,
    AxumPath(name): AxumPath<String>,
    Json(query): Json<QueryConfig>,
) -> Result<(), Error> {
    let mut config = config.lock().await;
    if !config.queries.contains_key(&name) {
        return Error::not_found().build();
    };

    let query_file = config.upsert_query(&name, &query)?;
    query_file.save()?;

    Ok(())
}

async fn delete_query(
    Extension(config): Extension<Arc<Mutex<Config>>>,
    AxumPath(name): AxumPath<String>,
) -> Result<(), Error> {
    let mut config = config.lock().await;
    if !config.queries.contains_key(&name) {
        return Error::not_found().build();
    }

    let query_file = config.delete_query(&name)?;
    query_file.save()?;

    Ok(())
}

async fn run_query(
    Extension(config): Extension<Arc<Mutex<Config>>>,
    AxumPath(name): AxumPath<String>,
) -> Result<Json<QueryResult>, Error> {
    let config_guard = config.lock().await;
    let sql = match config_guard.queries.get(&name) {
        Some(query) => query.sql.clone(),
        None => return Error::not_found().build(),
    };

    let data = execute_query(&config_guard, &sql).await?;
    let row_count = if data.is_empty() {
        0
    } else {
        data.values().next().map(|v| v.len()).unwrap_or(0)
    };
    let column_count = data.len();

    Ok(Json(QueryResult {
        data,
        row_count,
        column_count,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{api::StatusCode, test_helpers::TestManager};
    use anyhow::Result;
    use serde_json::json;

    #[tokio::test]
    async fn test_create_query() -> Result<()> {
        let test = TestManager::new();
        let server = test.setup_server(routes);

        let new_query = json!({
            "name": "test_query",
            "config": {
                "description": "Daily active users",
                "sql": "SELECT COUNT(*) as total FROM users WHERE active = true"
            }
        });

        let response = server.post("/queries").json(&new_query).await;
        response.assert_status_ok();

        let get_response = server.get("/queries/test_query").await;
        get_response.assert_status_ok();

        let query_config: QueryConfig = get_response.json();
        assert_eq!(
            query_config.description,
            Some("Daily active users".to_string())
        );
        assert_eq!(
            query_config.sql,
            "SELECT COUNT(*) as total FROM users WHERE active = true"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_update_query() -> Result<()> {
        let test = TestManager::new();
        let server = test.setup_server(routes);

        let original_query = QueryConfig {
            description: Some("Original description".to_string()),
            sql: "SELECT * FROM users".to_string(),
        };

        {
            let mut config = test.config().await;
            let query_file = config.upsert_query("test_query", &original_query)?;
            query_file.save()?;
        }

        let updated_config = json!({
            "description": "Updated description",
            "sql": "SELECT id, name FROM users WHERE status = 'active'"
        });

        let response = server
            .put("/queries/test_query")
            .json(&updated_config)
            .await;
        response.assert_status_ok();

        let get_response = server.get("/queries/test_query").await;
        get_response.assert_status_ok();

        let query_config: QueryConfig = get_response.json();
        assert_eq!(
            query_config.description,
            Some("Updated description".to_string())
        );
        assert!(query_config.sql.contains("WHERE status = 'active'"));

        Ok(())
    }

    #[tokio::test]
    async fn test_delete_query() -> Result<()> {
        let test = TestManager::new();
        let server = test.setup_server(routes);

        let query_config = QueryConfig {
            description: Some("This will be deleted".to_string()),
            sql: "SELECT * FROM test_table".to_string(),
        };

        {
            let mut config = test.config().await;
            let query_file = config.upsert_query("query_to_delete", &query_config)?;
            query_file.save()?;
        }

        let get_response = server.get("/queries/query_to_delete").await;
        get_response.assert_status_ok();

        let delete_response = server.delete("/queries/query_to_delete").await;
        delete_response.assert_status_ok();

        let get_response_after = server.get("/queries/query_to_delete").await;
        get_response_after.assert_status(StatusCode::NOT_FOUND);

        Ok(())
    }

    #[tokio::test]
    async fn test_list_queries() -> Result<()> {
        let test = TestManager::new();
        let server = test.setup_server(routes);

        let query1 = QueryConfig {
            description: Some("First description".to_string()),
            sql: "SELECT * FROM table1".to_string(),
        };

        let query2 = QueryConfig {
            description: None,
            sql: "SELECT * FROM table2".to_string(),
        };

        {
            let mut config = test.config().await;
            config.upsert_query("query1", &query1)?.save()?;
            config.upsert_query("query2", &query2)?.save()?;
        }

        let response = server.get("/queries").await;
        response.assert_status_ok();

        let query_list: QueryListResponse = response.json();
        assert_eq!(query_list.queries.len(), 2);

        let retrieved_query1 = query_list.queries.get("query1").unwrap();
        assert_eq!(
            retrieved_query1.description,
            Some("First description".to_string())
        );

        let retrieved_query2 = query_list.queries.get("query2").unwrap();
        assert_eq!(retrieved_query2.description, None);

        Ok(())
    }

    #[tokio::test]
    async fn test_adhoc_query_execution() -> Result<()> {
        let test = TestManager::new();
        let server = test.setup_server(routes);

        let query_request = json!({
            "sql": "SELECT 1 as test_column, 'hello' as message"
        });

        let response = server.post("/query").json(&query_request).await;
        response.assert_status_ok();

        let query_response: QueryResult = response.json();
        assert_eq!(query_response.column_count, 2);
        assert!(query_response.row_count > 0);

        Ok(())
    }

    #[tokio::test]
    async fn test_run_query() -> Result<()> {
        let test = TestManager::new();
        let server = test.setup_server(routes);

        let query_config = QueryConfig {
            description: Some("A test query".to_string()),
            sql: "SELECT 42 as answer, 'test' as label".to_string(),
        };

        {
            let mut config = test.config().await;
            let query_file = config.upsert_query("saved_query", &query_config)?;
            query_file.save()?;
        }

        let response = server.post("/queries/saved_query/run").await;
        response.assert_status_ok();

        let query_response: QueryResult = response.json();
        assert_eq!(query_response.column_count, 2);
        assert!(query_response.row_count > 0);

        Ok(())
    }

    #[tokio::test]
    async fn test_create_query_conflict() -> Result<()> {
        let test = TestManager::new();
        let server = test.setup_server(routes);

        let existing_query = QueryConfig {
            description: None,
            sql: "SELECT 1".to_string(),
        };

        {
            let mut config = test.config().await;
            let query_file = config.upsert_query("existing_query", &existing_query)?;
            query_file.save()?;
        }

        let duplicate_query = json!({
            "name": "existing_query",
            "config": {
                "description": "This should conflict",
                "sql": "SELECT 2"
            }
        });

        let response = server.post("/queries").json(&duplicate_query).await;
        response.assert_status(StatusCode::CONFLICT);

        Ok(())
    }

    #[tokio::test]
    async fn test_update_nonexistent_query() -> Result<()> {
        let test = TestManager::new();
        let server = test.setup_server(routes);

        let updated_config = json!({
            "description": "This doesn't exist",
            "sql": "SELECT * FROM nowhere"
        });

        let response = server
            .put("/queries/nonexistent_query")
            .json(&updated_config)
            .await;
        response.assert_status(StatusCode::NOT_FOUND);

        Ok(())
    }

    #[tokio::test]
    async fn test_delete_nonexistent_query() -> Result<()> {
        let test = TestManager::new();
        let server = test.setup_server(routes);

        let response = server.delete("/queries/nonexistent_query").await;
        response.assert_status(StatusCode::NOT_FOUND);

        Ok(())
    }

    #[tokio::test]
    async fn test_run_nonexistent_query() -> Result<()> {
        let test = TestManager::new();
        let server = test.setup_server(routes);

        let response = server.post("/queries/nonexistent_query/run").await;
        response.assert_status(StatusCode::NOT_FOUND);

        Ok(())
    }
}
