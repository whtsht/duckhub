use crate::core::{
    config::{Config, project::ProjectConfig},
    graph::Graph,
};
use axum::{Extension, Router};
use axum_test::TestServer;
use std::{path::Path, sync::Arc};
use tempfile::TempDir;
use tokio::sync::Mutex;

pub struct TestManager {
    temp_dir: TempDir,
    config: Arc<Mutex<Config>>,
    graph: Arc<Mutex<Graph>>,
}

impl TestManager {
    pub fn new() -> Self {
        let temp_dir = tempfile::tempdir().unwrap();
        let project_dir = temp_dir.path().to_path_buf();

        // Generate and save secret key for encryption tests
        let secret_key = crate::core::config::secret::generate_secret_key().unwrap();
        let key_path = project_dir.join(".secret.key");
        std::fs::write(key_path, secret_key).unwrap();

        let mut config = Config::new(project_dir.clone());
        config
            .add_project_setting(&ProjectConfig::default())
            .unwrap()
            .save()
            .unwrap();

        Self {
            temp_dir,
            config: Arc::new(Mutex::new(config)),
            graph: Arc::new(Mutex::new(Graph::new(&project_dir))),
        }
    }

    pub fn directory(&self) -> &Path {
        self.temp_dir.path()
    }

    pub async fn config(&self) -> tokio::sync::MutexGuard<'_, Config> {
        self.config.lock().await
    }

    pub async fn graph(&self) -> tokio::sync::MutexGuard<'_, Graph> {
        self.graph.lock().await
    }

    pub fn setup_server<F>(&self, routes: F) -> TestServer
    where
        F: FnOnce() -> Router,
    {
        let app = routes()
            .layer(Extension(self.config.clone()))
            .layer(Extension(self.graph.clone()));

        TestServer::new(app).unwrap()
    }
}

impl Default for TestManager {
    fn default() -> Self {
        Self::new()
    }
}
