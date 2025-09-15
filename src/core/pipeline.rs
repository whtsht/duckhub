use crate::core::{
    adapter::Adapter, config::Config, ducklake::DuckLake, graph::Graph, model::Model,
};
use anyhow::{Error, Result};
use chrono::{DateTime, Utc};
use futures::future;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio::{fs, sync::Mutex};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskStatus {
    phase: Phase,
    started_at: Option<DateTime<Utc>>,
    completed_at: Option<DateTime<Utc>>,
    error: Option<ErrorInfo>,
}

impl Default for TaskStatus {
    fn default() -> Self {
        Self::new()
    }
}

impl TaskStatus {
    fn new() -> Self {
        Self {
            started_at: None,
            phase: Phase::Waiting,
            completed_at: None,
            error: None,
        }
    }

    fn start(&mut self) {
        self.phase = Phase::Running;
        self.started_at = Some(Utc::now());
    }

    fn complete(&mut self) {
        self.phase = Phase::Completed;
        self.completed_at = Some(Utc::now());
        self.error = None;
    }

    fn fail(&mut self, error_message: String) {
        self.phase = Phase::Failed;
        self.error = Some(ErrorInfo {
            message: error_message,
            at: Utc::now(),
        });
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum Phase {
    Waiting,
    Running,
    Completed,
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorInfo {
    message: String,
    at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Pipeline {
    pub phase: Phase,
    pub started_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    pub tasks: HashMap<String, TaskStatus>,
    #[serde(skip)]
    filepath: PathBuf,
}

impl Pipeline {
    fn new(project_dir: &Path) -> Self {
        let pipeline_dir = Self::get_pipeline_dir(project_dir);
        let now = Utc::now();
        let filename = now.format("%Y-%m-%d-%H-%M-%S.json").to_string();
        let path = pipeline_dir.join(filename);

        Self {
            filepath: path,
            phase: Phase::Waiting,
            started_at: None,
            completed_at: None,
            tasks: HashMap::new(),
        }
    }

    fn to_datetime(path: &Path) -> Option<DateTime<Utc>> {
        if "json" != path.extension()?.to_str()? {
            return None;
        }

        let filename = path.file_stem()?.to_str()?;
        if let Ok(naive_time) = chrono::NaiveDateTime::parse_from_str(filename, "%Y-%m-%d-%H-%M-%S")
        {
            return Some(naive_time.and_utc());
        }

        None
    }

    pub async fn load_latest(project_dir: &Path) -> Result<Option<Self>> {
        let pipeline_dir = Self::get_pipeline_dir(project_dir);

        if !pipeline_dir.exists() {
            return Ok(None);
        }

        let mut entries = tokio::fs::read_dir(&pipeline_dir).await?;
        let mut latest_file: Option<PathBuf> = None;
        let mut latest_time: Option<DateTime<Utc>> = None;

        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            if let Some(time) = Self::to_datetime(&path) {
                if let Some(current_latest_time) = latest_time {
                    if time > current_latest_time {
                        latest_file = Some(path);
                        latest_time = Some(time);
                    }
                } else {
                    latest_file = Some(path);
                    latest_time = Some(time);
                }
            }
        }

        if let Some(path) = latest_file {
            let content = fs::read_to_string(&path).await?;
            let pipeline = serde_json::from_str(&content)?;
            Ok(Some(pipeline))
        } else {
            Ok(None)
        }
    }

    pub async fn load_all(project_dir: &Path) -> Result<Vec<Self>> {
        let pipeline_dir = Self::get_pipeline_dir(project_dir);

        if !pipeline_dir.exists() {
            return Ok(Vec::new());
        }

        let mut entries = tokio::fs::read_dir(&pipeline_dir).await?;
        let mut pipelines = Vec::new();

        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            if path.extension().and_then(|ext| ext.to_str()) == Some("json")
                && let Ok(content) = fs::read_to_string(&path).await
                && let Ok(mut pipeline) = serde_json::from_str::<Pipeline>(&content)
            {
                pipeline.filepath = path;
                pipelines.push(pipeline);
            }
        }

        pipelines.sort_by(|a, b| {
            let time_a = Self::to_datetime(&a.filepath);
            let time_b = Self::to_datetime(&b.filepath);
            match (time_a, time_b) {
                (Some(ta), Some(tb)) => tb.cmp(&ta),
                (Some(_), None) => std::cmp::Ordering::Less,
                (None, Some(_)) => std::cmp::Ordering::Greater,
                (None, None) => std::cmp::Ordering::Equal,
            }
        });

        Ok(pipelines)
    }

    async fn start(&mut self, tasks: &[String]) -> Result<()> {
        self.phase = Phase::Running;
        self.started_at = Some(Utc::now());
        self.completed_at = None;
        self.tasks = HashMap::from_iter(
            tasks
                .iter()
                .cloned()
                .map(|table| (table, TaskStatus::new())),
        );

        self.save().await
    }

    fn get_pipeline_dir(project_dir: &Path) -> PathBuf {
        project_dir.join(".data").join("pipelines")
    }

    async fn complete(&mut self) -> Result<()> {
        self.phase = Phase::Completed;
        self.completed_at = Some(Utc::now());
        self.save().await
    }

    async fn waiting_task(&self) -> Vec<String> {
        self.tasks
            .iter()
            .filter(|(_, task)| task.phase == Phase::Waiting)
            .map(|(name, _)| name.to_string())
            .collect()
    }

    fn all_deps_completed(&self, dependencies: &[String]) -> bool {
        dependencies.iter().all(|dep| {
            self.tasks
                .get(dep)
                .map(|task| task.phase == Phase::Completed)
                .unwrap_or(false)
        })
    }

    async fn start_task(&mut self, name: &str) -> Result<()> {
        if let Some(task) = self.tasks.get_mut(name) {
            task.start();
        }
        self.save().await
    }

    async fn complete_task(&mut self, name: &str) -> Result<()> {
        if let Some(task) = self.tasks.get_mut(name) {
            task.complete();
        }
        self.save().await
    }

    async fn fail_task(&mut self, name: &str, error_message: String) -> Result<()> {
        if let Some(task) = self.tasks.get_mut(name) {
            task.fail(error_message);
        }
        self.save().await
    }

    async fn save(&self) -> Result<()> {
        let content = serde_json::to_string_pretty(&self)?;
        if let Some(dir) = self.filepath.parent() {
            fs::create_dir_all(dir).await?;
        }
        fs::write(&self.filepath, content).await?;
        Ok(())
    }
}

#[async_trait::async_trait]
pub trait TaskExecutor: Send + Sync {
    async fn run(&self, name: &str) -> Result<()>;
}

pub struct Executor {
    ducklake: Arc<DuckLake>,
    config: Arc<Mutex<Config>>,
}

#[async_trait::async_trait]
impl TaskExecutor for Executor {
    async fn run(&self, name: &str) -> Result<()> {
        let config = self.config.lock().await;
        if let Some(adapter_config) = config.adapters.get(name) {
            let adapter = Adapter::new(adapter_config.clone(), Arc::clone(&self.ducklake));
            adapter.import(name, &config.project.connections).await
        } else if let Some(model_config) = config.models.get(name) {
            let model = Model::new(model_config.clone(), Arc::clone(&self.ducklake));
            model.transform(name).await
        } else {
            Err(anyhow::anyhow!("Unknown task: {}", name))
        }
    }
}

#[derive(Clone)]
pub struct Worker<T: TaskExecutor> {
    pipeline: Arc<Mutex<Pipeline>>,
    executor: Arc<T>,
    graph: Arc<Mutex<Graph>>,
}

impl<T: TaskExecutor> Worker<T> {
    fn new(pipeline: Arc<Mutex<Pipeline>>, executor: Arc<T>, graph: Arc<Mutex<Graph>>) -> Self {
        Self {
            pipeline,
            executor,
            graph,
        }
    }

    async fn complete_task(&self, name: &str) -> Result<()> {
        let mut pipeline = self.pipeline.lock().await;
        pipeline.complete_task(name).await?;
        let mut graph = self.graph.lock().await;
        graph.update(name);
        graph.save().await
    }

    async fn fail_task(&self, name: &str, error: Error) -> Result<()> {
        {
            let mut pipeline = self.pipeline.lock().await;
            pipeline.fail_task(name, format!("{error:?}")).await?;
        }

        let downstream = self.graph.lock().await.downstream(name);
        for task in downstream {
            let mut pipeline = self.pipeline.lock().await;
            pipeline
                .fail_task(&task, "Upstream task failed".to_string())
                .await?;
        }

        Ok(())
    }

    async fn run(&self) -> Result<()> {
        loop {
            let Some(name) = pop_task(&self.graph, &self.pipeline).await? else {
                break;
            };

            use tokio::time::{Duration, sleep};
            sleep(Duration::from_secs(2)).await;

            match self.executor.run(&name).await {
                Ok(()) => {
                    self.complete_task(&name).await?;
                }
                Err(error) => {
                    self.fail_task(&name, error).await?;
                }
            }
        }
        Ok(())
    }
}

pub async fn run_pipeline_all(config: Arc<Mutex<Config>>, graph: Arc<Mutex<Graph>>) -> Result<()> {
    let (tasks, pipeline, executor) = {
        let config_guard = config.lock().await;
        let tasks = config_guard
            .adapters
            .keys()
            .chain(config_guard.models.keys())
            .cloned()
            .collect::<Vec<String>>();
        let pipeline = Arc::new(Mutex::new(Pipeline::new(&config_guard.project_dir)));
        let ducklake = Arc::new(DuckLake::from_config(&config_guard).await?);
        drop(config_guard);
        let executor = Arc::new(Executor {
            ducklake,
            config: config.clone(),
        });
        (tasks, pipeline, executor)
    };
    run_pipeline(executor, graph, pipeline, &tasks).await
}

pub async fn run_pipeline_node(
    config: Arc<Mutex<Config>>,
    graph: Arc<Mutex<Graph>>,
    node_name: String,
) -> Result<()> {
    let (tasks, pipeline, executor) = {
        let config_guard = config.lock().await;
        let graph_guard = graph.lock().await;

        let mut visited = std::collections::HashSet::new();
        visited.insert(node_name.clone());
        let mut upstream_tasks = graph_guard.all_upstream(&node_name, &mut visited);
        upstream_tasks.push(node_name.clone());

        let pipeline = Arc::new(Mutex::new(Pipeline::new(&config_guard.project_dir)));
        let ducklake = Arc::new(DuckLake::from_config(&config_guard).await?);
        drop(config_guard);
        drop(graph_guard);
        let executor = Arc::new(Executor {
            ducklake,
            config: config.clone(),
        });
        (upstream_tasks, pipeline, executor)
    };
    run_pipeline(executor, graph, pipeline, &tasks).await
}

pub async fn run_pipeline<T: TaskExecutor + 'static>(
    executor: Arc<T>,
    graph: Arc<Mutex<Graph>>,
    pipeline: Arc<Mutex<Pipeline>>,
    tasks: &[String],
) -> Result<()> {
    {
        let mut pipeline = pipeline.lock().await;
        pipeline.start(tasks).await?;
    }

    let worker_count = num_cpus::get();
    let handles = (0..worker_count).map(|_| {
        let worker = Worker::new(pipeline.clone(), executor.clone(), graph.clone());

        tokio::spawn(async move { worker.run().await })
    });

    future::try_join_all(handles).await?;

    {
        let mut pipeline = pipeline.lock().await;
        pipeline.complete().await?;
    }

    Ok(())
}

pub async fn pop_task(
    graph: &Arc<Mutex<Graph>>,
    pipeline: &Arc<Mutex<Pipeline>>,
) -> Result<Option<String>> {
    let mut pipeline = pipeline.lock().await;
    let graph = graph.lock().await;
    let waiting_tasks = pipeline.waiting_task().await;

    for task_name in waiting_tasks {
        let empty_deps = vec![];
        let dependencies = graph
            .nodes
            .get(&task_name)
            .map(|node| &node.dependencies)
            .unwrap_or(&empty_deps);

        if pipeline.all_deps_completed(dependencies) {
            pipeline.start_task(&task_name).await?;
            return Ok(Some(task_name));
        }
    }

    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::TaskExecutor;
    use anyhow::Result;

    #[tokio::test]
    async fn test_pipeline() -> Result<()> {
        use super::*;
        let tempdir = tempfile::tempdir()?;
        let testdir = tempdir.path();

        let result = Pipeline::load_latest(testdir).await?;
        assert!(result.is_none());

        let mut manager = Pipeline::new(testdir);

        let tasks = vec![
            "a".to_string(),
            "b".to_string(),
            "c".to_string(),
            "d".to_string(),
            "e".to_string(),
        ];
        manager.start(&tasks).await?;

        let result = Pipeline::load_latest(testdir).await?;
        assert!(result.is_some());
        let pipeline = result.unwrap();
        assert_eq!(pipeline.phase, Phase::Running);
        assert_eq!(pipeline.tasks.len(), 5);

        let waiting = manager.waiting_task().await;
        assert_eq!(waiting.len(), 5);
        assert!(waiting.contains(&"a".to_string()));
        assert!(waiting.contains(&"b".to_string()));
        assert!(waiting.contains(&"c".to_string()));
        assert!(waiting.contains(&"d".to_string()));
        assert!(waiting.contains(&"e".to_string()));

        manager.start_task("a").await?;
        manager.start_task("b").await?;
        manager.start_task("c").await?;

        let waiting = manager.waiting_task().await;
        assert_eq!(waiting.len(), 2);
        assert!(waiting.contains(&"d".to_string()));
        assert!(waiting.contains(&"e".to_string()));

        let deps = vec!["a".to_string(), "b".to_string()];
        assert!(!manager.all_deps_completed(&deps));

        manager.complete_task("a").await?;
        manager.complete_task("b").await?;

        let waiting = manager.waiting_task().await;
        assert_eq!(waiting.len(), 2);
        assert!(waiting.contains(&"d".to_string()));
        assert!(waiting.contains(&"e".to_string()));

        let deps = vec!["a".to_string(), "b".to_string()];
        assert!(manager.all_deps_completed(&deps));

        Ok(())
    }

    #[tokio::test]
    async fn test_pipeline_success() -> Result<()> {
        use super::*;
        use tempfile::tempdir;

        let tempdir = tempdir()?;
        let project_dir = tempdir.path();

        let mock_executor = Arc::new(MockExecutor {
            success_tasks: vec![
                "a".to_string(),
                "b".to_string(),
                "c".to_string(),
                "e".to_string(),
                "d".to_string(),
            ],
            fail_tasks: vec![],
        });

        let mut graph = Graph::default();
        // a-->b-->c
        //     |
        // e---+-->d
        graph.create_node("a", &[]);
        graph.create_node("e", &[]);
        graph.create_node("b", &["a"]);
        graph.create_node("c", &["b"]);
        graph.create_node("d", &["b", "e"]);

        let graph = Arc::new(Mutex::new(graph));
        let pipeline = Arc::new(Mutex::new(Pipeline::new(project_dir)));

        let tasks = vec![
            "a".to_string(),
            "b".to_string(),
            "c".to_string(),
            "e".to_string(),
            "d".to_string(),
        ];

        run_pipeline(mock_executor, graph, pipeline.clone(), &tasks).await?;

        let pipeline_guard = pipeline.lock().await;
        assert_eq!(pipeline_guard.phase, Phase::Completed);
        assert_eq!(
            pipeline_guard.tasks.get("a").unwrap().phase,
            Phase::Completed
        );
        assert_eq!(
            pipeline_guard.tasks.get("b").unwrap().phase,
            Phase::Completed
        );
        assert_eq!(
            pipeline_guard.tasks.get("c").unwrap().phase,
            Phase::Completed
        );
        assert_eq!(
            pipeline_guard.tasks.get("e").unwrap().phase,
            Phase::Completed
        );
        assert_eq!(
            pipeline_guard.tasks.get("d").unwrap().phase,
            Phase::Completed
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_pipeline_failure() -> Result<()> {
        use super::*;
        use tempfile::tempdir;

        let tempdir = tempdir()?;
        let project_dir = tempdir.path();

        let mock_executor = Arc::new(MockExecutor {
            success_tasks: vec!["a".to_string(), "b".to_string(), "c".to_string()],
            fail_tasks: vec!["e".to_string()],
        });

        let mut graph = Graph::default();
        // a-->b-->c
        //     |
        // e---+-->d
        graph.create_node("a", &[]);
        graph.create_node("e", &[]);
        graph.create_node("b", &["a"]);
        graph.create_node("c", &["b"]);
        graph.create_node("d", &["b", "e"]);

        let graph = Arc::new(Mutex::new(graph));
        let pipeline = Arc::new(Mutex::new(Pipeline::new(project_dir)));

        let tasks = vec![
            "a".to_string(),
            "b".to_string(),
            "c".to_string(),
            "e".to_string(),
            "d".to_string(),
        ];

        run_pipeline(mock_executor, graph, pipeline.clone(), &tasks).await?;

        let pipeline_guard = pipeline.lock().await;
        assert_eq!(pipeline_guard.phase, Phase::Completed);
        assert_eq!(
            pipeline_guard.tasks.get("a").unwrap().phase,
            Phase::Completed
        );
        assert_eq!(
            pipeline_guard.tasks.get("b").unwrap().phase,
            Phase::Completed
        );
        assert_eq!(
            pipeline_guard.tasks.get("c").unwrap().phase,
            Phase::Completed
        );
        assert_eq!(pipeline_guard.tasks.get("e").unwrap().phase, Phase::Failed);
        assert_eq!(pipeline_guard.tasks.get("d").unwrap().phase, Phase::Failed);

        Ok(())
    }

    pub struct MockExecutor {
        pub success_tasks: Vec<String>,
        pub fail_tasks: Vec<String>,
    }

    #[async_trait::async_trait]
    impl TaskExecutor for MockExecutor {
        async fn run(&self, name: &str) -> Result<()> {
            if self.success_tasks.contains(&name.to_string()) {
                Ok(())
            } else if self.fail_tasks.contains(&name.to_string()) {
                Err(anyhow::anyhow!("Task {} failed", name))
            } else {
                Err(anyhow::anyhow!("Unknown task: {}", name))
            }
        }
    }
}
