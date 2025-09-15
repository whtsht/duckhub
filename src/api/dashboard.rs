use crate::{
    api::Error,
    core::{
        config::{
            Config,
            dashboard::{ChartType, DashboardConfig},
        },
        ducklake::DuckLake,
    },
};
use axum::{Extension, Router, extract::Path, response::Json, routing::get};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Deserialize)]
pub struct CreateDashboardRequest {
    pub name: String,
    pub config: DashboardConfig,
}

pub fn router() -> Router {
    Router::new()
        .route("/dashboards", get(list_dashboards).post(create_dashboard))
        .route(
            "/dashboards/{name}",
            get(get_dashboard)
                .put(update_dashboard)
                .delete(delete_dashboard),
        )
        .route("/dashboards/{name}/data", get(get_dashboard_data))
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DashboardListItem {
    pub name: String,
    pub description: Option<String>,
    pub query: String,
    pub chart_type: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DashboardRequest {
    pub name: String,
    pub description: Option<String>,
    pub query: String,
    pub chart: ChartRequest,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ChartRequest {
    #[serde(rename = "type")]
    pub chart_type: String,
    pub x_column: String,
    pub y_column: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DashboardDataResponse {
    pub labels: Vec<serde_json::Value>,
    pub values: Vec<serde_json::Value>,
}

async fn list_dashboards(
    Extension(config): Extension<Arc<Mutex<Config>>>,
) -> Result<Json<Vec<DashboardListItem>>, Error> {
    let config = config.lock().await;
    let dashboards: Vec<DashboardListItem> = config
        .dashboards
        .iter()
        .map(|(name, dashboard_config)| DashboardListItem {
            name: name.clone(),
            description: dashboard_config.description.clone(),
            query: dashboard_config.query.clone(),
            chart_type: match dashboard_config.chart.chart_type {
                ChartType::Line => "line".to_string(),
                ChartType::Bar => "bar".to_string(),
            },
        })
        .collect();
    Ok(Json(dashboards))
}

async fn get_dashboard(
    Extension(config): Extension<Arc<Mutex<Config>>>,
    Path(name): Path<String>,
) -> Result<Json<DashboardConfig>, Error> {
    let config = config.lock().await;
    if let Some(dashboard_config) = config.dashboards.get(&name) {
        Ok(Json(dashboard_config.clone()))
    } else {
        Error::not_found().build()
    }
}

async fn create_dashboard(
    Extension(config): Extension<Arc<Mutex<Config>>>,
    Json(request): Json<CreateDashboardRequest>,
) -> Result<(), Error> {
    let mut config = config.lock().await;
    if config.dashboards.contains_key(&request.name) {
        return Error::conflict().build();
    }

    let dashboard_file = config.upsert_dashboard(&request.name, &request.config)?;
    dashboard_file.save()?;

    Ok(())
}

async fn update_dashboard(
    Extension(config): Extension<Arc<Mutex<Config>>>,
    Path(name): Path<String>,
    Json(dashboard): Json<DashboardConfig>,
) -> Result<(), Error> {
    let mut config = config.lock().await;
    if !config.dashboards.contains_key(&name) {
        return Error::not_found().build();
    }

    let dashboard_file = config.upsert_dashboard(&name, &dashboard)?;
    dashboard_file.save()?;

    Ok(())
}

async fn delete_dashboard(
    Extension(config): Extension<Arc<Mutex<Config>>>,
    Path(name): Path<String>,
) -> Result<(), Error> {
    let mut config = config.lock().await;
    if !config.dashboards.contains_key(&name) {
        return Error::not_found().build();
    }

    let dashboard_file = config.delete_dashboard(&name)?;
    dashboard_file.save()?;

    Ok(())
}

async fn get_dashboard_data(
    Extension(config): Extension<Arc<Mutex<Config>>>,
    Path(name): Path<String>,
) -> Result<Json<DashboardDataResponse>, Error> {
    let config = config.lock().await;
    let dashboard_config = match config.dashboards.get(&name) {
        Some(c) => c,
        None => return Error::not_found().build(),
    };

    let query_config = match config.queries.get(&dashboard_config.query) {
        Some(q) => q,
        None => return Error::not_found().build(),
    };

    let ducklake = DuckLake::from_config(&config).await?;

    let describe_sql = format!("DESCRIBE ({})", query_config.sql);
    let describe_results = ducklake.query(&describe_sql)?;

    let mut x_column_index = None;
    let mut y_column_index = None;

    for (idx, describe_row) in describe_results.iter().enumerate() {
        if !describe_row.is_empty() {
            let column_name = &describe_row[0];
            if column_name == &dashboard_config.chart.x_column {
                x_column_index = Some(idx);
            }
            if column_name == &dashboard_config.chart.y_column {
                y_column_index = Some(idx);
            }
        }
    }

    let x_idx = match x_column_index {
        Some(idx) => idx,
        None => return Error::bad_request().build(),
    };
    let y_idx = match y_column_index {
        Some(idx) => idx,
        None => return Error::bad_request().build(),
    };

    let query_results = ducklake.query(&query_config.sql)?;

    let mut labels = Vec::new();
    let mut values = Vec::new();

    for row in query_results {
        if row.len() > x_idx && row.len() > y_idx {
            labels.push(serde_json::Value::String(row[x_idx].clone()));
            if let Ok(num) = row[y_idx].parse::<f64>() {
                values.push(serde_json::Value::Number(
                    serde_json::Number::from_f64(num)
                        .unwrap_or_else(|| serde_json::Number::from(0)),
                ));
            } else {
                values.push(serde_json::Value::String(row[y_idx].clone()));
            }
        }
    }

    Ok(Json(DashboardDataResponse { labels, values }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        api::StatusCode,
        core::config::{
            dashboard::{ChartConfig, ChartType},
            query::QueryConfig,
        },
        test_helpers::TestManager,
    };
    use anyhow::Result;
    use serde_json::json;

    #[tokio::test]
    async fn test_create_dashboard_line_chart() -> Result<()> {
        let test = TestManager::new();
        let server = test.setup_server(router);

        let new_dashboard = json!({
            "name": "test_dashboard",
            "config": {
                "description": "Monthly sales overview",
                "query": "monthly_sales",
                "chart": {
                    "type": "line",
                    "x_column": "month",
                    "y_column": "revenue"
                }
            }
        });

        let response = server.post("/dashboards").json(&new_dashboard).await;
        response.assert_status_ok();

        let get_response = server.get("/dashboards/test_dashboard").await;
        get_response.assert_status_ok();

        let dashboard_config: DashboardConfig = get_response.json();
        assert_eq!(
            dashboard_config.description,
            Some("Monthly sales overview".to_string())
        );
        assert_eq!(dashboard_config.query, "monthly_sales");
        assert_eq!(dashboard_config.chart.chart_type, ChartType::Line);
        assert_eq!(dashboard_config.chart.x_column, "month");
        assert_eq!(dashboard_config.chart.y_column, "revenue");

        Ok(())
    }

    #[tokio::test]
    async fn test_create_dashboard_bar_chart() -> Result<()> {
        let test = TestManager::new();
        let server = test.setup_server(router);

        let new_dashboard = json!({
            "name": "category_dashboard",
            "config": {
                "query": "category_breakdown",
                "chart": {
                    "type": "bar",
                    "x_column": "category",
                    "y_column": "total_amount"
                }
            }
        });

        let response = server.post("/dashboards").json(&new_dashboard).await;
        response.assert_status_ok();

        let get_response = server.get("/dashboards/category_dashboard").await;
        get_response.assert_status_ok();

        let dashboard_config: DashboardConfig = get_response.json();
        assert_eq!(dashboard_config.description, None);
        assert_eq!(dashboard_config.query, "category_breakdown");
        assert_eq!(dashboard_config.chart.chart_type, ChartType::Bar);
        assert_eq!(dashboard_config.chart.x_column, "category");
        assert_eq!(dashboard_config.chart.y_column, "total_amount");

        Ok(())
    }

    #[tokio::test]
    async fn test_update_dashboard() -> Result<()> {
        let test = TestManager::new();
        let server = test.setup_server(router);

        let original_dashboard = DashboardConfig {
            description: Some("Original description".to_string()),
            query: "original_query".to_string(),
            chart: ChartConfig {
                chart_type: ChartType::Line,
                x_column: "x".to_string(),
                y_column: "y".to_string(),
            },
        };

        {
            let mut config = test.config().await;
            let dashboard_file = config.upsert_dashboard("test_dashboard", &original_dashboard)?;
            dashboard_file.save()?;
        }

        let updated_config = json!({
            "description": "Updated description",
            "query": "updated_query",
            "chart": {
                "type": "bar",
                "x_column": "new_x",
                "y_column": "new_y"
            }
        });

        let response = server
            .put("/dashboards/test_dashboard")
            .json(&updated_config)
            .await;
        response.assert_status_ok();

        let get_response = server.get("/dashboards/test_dashboard").await;
        get_response.assert_status_ok();

        let dashboard_config: DashboardConfig = get_response.json();
        assert_eq!(
            dashboard_config.description,
            Some("Updated description".to_string())
        );
        assert_eq!(dashboard_config.query, "updated_query");
        assert_eq!(dashboard_config.chart.chart_type, ChartType::Bar);
        assert_eq!(dashboard_config.chart.x_column, "new_x");
        assert_eq!(dashboard_config.chart.y_column, "new_y");

        Ok(())
    }

    #[tokio::test]
    async fn test_delete_dashboard() -> Result<()> {
        let test = TestManager::new();
        let server = test.setup_server(router);

        let dashboard_config = DashboardConfig {
            description: Some("This will be deleted".to_string()),
            query: "test_query".to_string(),
            chart: ChartConfig {
                chart_type: ChartType::Line,
                x_column: "x".to_string(),
                y_column: "y".to_string(),
            },
        };

        {
            let mut config = test.config().await;
            let dashboard_file =
                config.upsert_dashboard("dashboard_to_delete", &dashboard_config)?;
            dashboard_file.save()?;
        }

        let get_response = server.get("/dashboards/dashboard_to_delete").await;
        get_response.assert_status_ok();

        let delete_response = server.delete("/dashboards/dashboard_to_delete").await;
        delete_response.assert_status_ok();

        let get_response_after = server.get("/dashboards/dashboard_to_delete").await;
        get_response_after.assert_status(StatusCode::NOT_FOUND);

        Ok(())
    }

    #[tokio::test]
    async fn test_list_dashboards() -> Result<()> {
        let test = TestManager::new();
        let server = test.setup_server(router);

        let dashboard1 = DashboardConfig {
            description: Some("First description".to_string()),
            query: "query1".to_string(),
            chart: ChartConfig {
                chart_type: ChartType::Line,
                x_column: "x1".to_string(),
                y_column: "y1".to_string(),
            },
        };

        let dashboard2 = DashboardConfig {
            description: None,
            query: "query2".to_string(),
            chart: ChartConfig {
                chart_type: ChartType::Bar,
                x_column: "x2".to_string(),
                y_column: "y2".to_string(),
            },
        };

        {
            let mut config = test.config().await;
            config.upsert_dashboard("dashboard1", &dashboard1)?.save()?;
            config.upsert_dashboard("dashboard2", &dashboard2)?.save()?;
        }

        let response = server.get("/dashboards").await;
        response.assert_status_ok();

        let dashboards: Vec<DashboardListItem> = response.json();
        assert_eq!(dashboards.len(), 2);

        let dashboard1_item = dashboards.iter().find(|d| d.name == "dashboard1").unwrap();
        assert_eq!(
            dashboard1_item.description,
            Some("First description".to_string())
        );
        assert_eq!(dashboard1_item.query, "query1");
        assert_eq!(dashboard1_item.chart_type, "line");

        let dashboard2_item = dashboards.iter().find(|d| d.name == "dashboard2").unwrap();
        assert_eq!(dashboard2_item.description, None);
        assert_eq!(dashboard2_item.query, "query2");
        assert_eq!(dashboard2_item.chart_type, "bar");

        Ok(())
    }

    #[tokio::test]
    async fn test_dashboard_data_retrieval() -> Result<()> {
        let test = TestManager::new();
        let server = test.setup_server(router);

        let query_config = QueryConfig {
            description: Some("Query for dashboard data".to_string()),
            sql: "SELECT 'Jan' as month, 1000 as revenue UNION SELECT 'Feb' as month, 1500 as revenue".to_string(),
        };

        let dashboard_config = DashboardConfig {
            description: Some("Dashboard for testing data".to_string()),
            query: "test_data_query".to_string(),
            chart: ChartConfig {
                chart_type: ChartType::Line,
                x_column: "month".to_string(),
                y_column: "revenue".to_string(),
            },
        };

        {
            let mut config = test.config().await;
            config
                .upsert_query("test_data_query", &query_config)?
                .save()?;
            config
                .upsert_dashboard("test_data_dashboard", &dashboard_config)?
                .save()?;
        }

        let response = server.get("/dashboards/test_data_dashboard/data").await;
        response.assert_status_ok();

        let data_response: DashboardDataResponse = response.json();
        assert!(!data_response.labels.is_empty());
        assert!(!data_response.values.is_empty());
        assert_eq!(data_response.labels.len(), data_response.values.len());

        Ok(())
    }

    #[tokio::test]
    async fn test_create_dashboard_conflict() -> Result<()> {
        let test = TestManager::new();
        let server = test.setup_server(router);

        let existing_dashboard = DashboardConfig {
            description: None,
            query: "test_query".to_string(),
            chart: ChartConfig {
                chart_type: ChartType::Line,
                x_column: "x".to_string(),
                y_column: "y".to_string(),
            },
        };

        {
            let mut config = test.config().await;
            let dashboard_file =
                config.upsert_dashboard("existing_dashboard", &existing_dashboard)?;
            dashboard_file.save()?;
        }

        let duplicate_dashboard = json!({
            "name": "existing_dashboard",
            "config": {
                "description": "This should conflict",
                "query": "another_query",
                "chart": {
                    "type": "bar",
                    "x_column": "x",
                    "y_column": "y"
                }
            }
        });

        let response = server.post("/dashboards").json(&duplicate_dashboard).await;
        response.assert_status(StatusCode::CONFLICT);

        Ok(())
    }

    #[tokio::test]
    async fn test_update_nonexistent_dashboard() -> Result<()> {
        let test = TestManager::new();
        let server = test.setup_server(router);

        let updated_config = json!({
            "description": "This doesn't exist",
            "query": "nonexistent_query",
            "chart": {
                "type": "line",
                "x_column": "x",
                "y_column": "y"
            }
        });

        let response = server
            .put("/dashboards/nonexistent_dashboard")
            .json(&updated_config)
            .await;
        response.assert_status(StatusCode::NOT_FOUND);

        Ok(())
    }

    #[tokio::test]
    async fn test_delete_nonexistent_dashboard() -> Result<()> {
        let test = TestManager::new();
        let server = test.setup_server(router);

        let response = server.delete("/dashboards/nonexistent_dashboard").await;
        response.assert_status(StatusCode::NOT_FOUND);

        Ok(())
    }

    #[tokio::test]
    async fn test_dashboard_data_nonexistent_dashboard() -> Result<()> {
        let test = TestManager::new();
        let server = test.setup_server(router);

        let response = server.get("/dashboards/nonexistent_dashboard/data").await;
        response.assert_status(StatusCode::NOT_FOUND);

        Ok(())
    }

    #[tokio::test]
    async fn test_dashboard_data_nonexistent_query() -> Result<()> {
        let test = TestManager::new();
        let server = test.setup_server(router);

        let dashboard_config = DashboardConfig {
            description: None,
            query: "missing_query".to_string(),
            chart: ChartConfig {
                chart_type: ChartType::Line,
                x_column: "x".to_string(),
                y_column: "y".to_string(),
            },
        };

        {
            let mut config = test.config().await;
            let dashboard_file =
                config.upsert_dashboard("dashboard_with_missing_query", &dashboard_config)?;
            dashboard_file.save()?;
        }

        let response = server
            .get("/dashboards/dashboard_with_missing_query/data")
            .await;
        response.assert_status(StatusCode::NOT_FOUND);

        Ok(())
    }
}
