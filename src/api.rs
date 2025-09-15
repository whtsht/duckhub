use crate::core::{config::Config, graph::Graph};
use anyhow::Result;
pub use axum::http::StatusCode;
use axum::{
    Extension, Router,
    response::{IntoResponse, Response},
};
use std::sync::Arc;
use tokio::sync::Mutex;
use tower_http::cors::{Any, CorsLayer};

mod adapter;
mod connection;
mod dashboard;
mod graph;
mod model;
mod pipeline;
mod query;

#[derive(Debug)]
pub struct Error {
    status_code: StatusCode,
    message: Option<String>,
}

impl Error {
    pub fn new(status_code: StatusCode) -> Self {
        Self {
            status_code,
            message: None,
        }
    }

    pub fn with_message(mut self, message: impl Into<String>) -> Self {
        self.message = Some(message.into());
        self
    }

    pub fn bad_request() -> Self {
        Self::new(StatusCode::BAD_REQUEST)
    }

    pub fn not_found() -> Self {
        Self::new(StatusCode::NOT_FOUND)
    }

    pub fn internal_server_error() -> Self {
        Self::new(StatusCode::INTERNAL_SERVER_ERROR)
    }

    pub fn conflict() -> Self {
        Self::new(StatusCode::CONFLICT)
    }

    pub fn build<T>(self) -> Result<T, Self> {
        Err(self)
    }
}

impl IntoResponse for Error {
    fn into_response(self) -> Response {
        println!("Error: {:?}", self);
        let body = serde_json::json!({
            "message": self.message.unwrap_or_else(|| "An error occurred".to_string())
        });
        let body_string = serde_json::to_string(&body).expect("failed parse response");
        (
            self.status_code,
            [("content-type", "application/json")],
            body_string,
        )
            .into_response()
    }
}

impl<E> From<E> for Error
where
    E: Into<anyhow::Error>,
{
    fn from(err: E) -> Self {
        let err: anyhow::Error = err.into();
        Self {
            status_code: StatusCode::INTERNAL_SERVER_ERROR,
            message: Some(err.to_string()),
        }
    }
}

pub async fn main(config: Config) -> Result<()> {
    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);

    let routes = Router::new()
        .merge(adapter::routes())
        .merge(connection::routes())
        .merge(dashboard::router())
        .merge(model::routes())
        .merge(query::routes())
        .merge(graph::routes())
        .merge(pipeline::routes());

    let graph = Graph::load(&config.project_dir).await?;

    let app = Router::new()
        .nest("/api", routes)
        .layer(cors)
        .layer(Extension(Arc::new(Mutex::new(graph))))
        .layer(Extension(Arc::new(Mutex::new(config))));

    let port = 3015;
    let listener = tokio::net::TcpListener::bind(format!("localhost:{port}")).await?;

    axum::serve(listener, app).await?;

    Ok(())
}
