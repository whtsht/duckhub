use crate::{
    api::Error,
    core::{
        config::Config,
        graph::Graph,
        pipeline::{Pipeline, run_pipeline_all, run_pipeline_node},
    },
};
use anyhow::Result;
use axum::{
    Extension, Router,
    extract::Json as ExtractJson,
    response::Json,
    routing::{get, post},
};
use serde::Deserialize;
use std::sync::Arc;
use tokio::sync::Mutex;

pub fn routes() -> Router {
    Router::new()
        .route("/pipelines", get(list_pipelines))
        .route("/pipeline", get(get_pipeline))
        .route("/pipeline/run", post(run))
        .route("/pipeline/run-node", post(run_node))
}

async fn list_pipelines(
    Extension(config): Extension<Arc<Mutex<Config>>>,
) -> Result<Json<Vec<Pipeline>>, Error> {
    let project_dir = {
        let config = config.lock().await;
        config.project_dir.clone()
    };

    let pipelines = Pipeline::load_all(&project_dir).await?;

    Ok(Json(pipelines))
}

async fn get_pipeline(
    Extension(config): Extension<Arc<Mutex<Config>>>,
) -> Result<Json<Option<Pipeline>>, Error> {
    let project_dir = {
        let config = config.lock().await;
        config.project_dir.clone()
    };

    let status = Pipeline::load_latest(&project_dir).await?;

    Ok(Json(status))
}

async fn run(
    Extension(graph): Extension<Arc<Mutex<Graph>>>,
    Extension(config): Extension<Arc<Mutex<Config>>>,
) -> Result<(), Error> {
    tokio::spawn(async move { run_pipeline_all(config, graph).await });
    Ok(())
}

#[derive(Deserialize)]
struct RunNodeRequest {
    node_name: String,
}

async fn run_node(
    Extension(graph): Extension<Arc<Mutex<Graph>>>,
    Extension(config): Extension<Arc<Mutex<Config>>>,
    ExtractJson(request): ExtractJson<RunNodeRequest>,
) -> Result<(), Error> {
    let node_name = request.node_name;
    tokio::spawn(async move { run_pipeline_node(config, graph, node_name).await });
    Ok(())
}
