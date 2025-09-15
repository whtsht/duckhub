use crate::{
    api::Error,
    core::config::{Config, project::ConnectionConfig, secret::SecretField},
};
use anyhow::Result;
use axum::{
    Extension, Router,
    extract::Path,
    response::Json,
    routing::{get, post},
};
use serde::{Deserialize, Serialize};
use std::{path::Path as StdPath, sync::Arc};
use tokio::sync::Mutex;

#[derive(Serialize, Deserialize)]
pub struct ConnectionSummary {
    pub name: String,
    pub connection_type: String,
    pub details: String,
}

#[derive(Deserialize)]
pub struct CreateConnectionRequest {
    pub name: String,
    pub config: ConnectionConfig,
}

#[derive(Deserialize, Serialize, Debug)]
#[serde(tag = "type")]
pub enum TestConnectionConfig {
    #[serde(rename = "sqlite")]
    SQLite { path: String },
    #[serde(rename = "localfile")]
    LocalFile { base_path: String },
    #[serde(rename = "mysql")]
    MySQL {
        host: String,
        port: u16,
        database: String,
        username: String,
        password: String,
    },
    #[serde(rename = "postgresql")]
    PostgreSQL {
        host: String,
        port: u16,
        database: String,
        username: String,
        password: String,
    },
    #[serde(rename = "s3")]
    S3 {
        bucket: String,
        region: String,
        endpoint_url: Option<String>,
        auth_method: String,
        access_key_id: Option<String>,
        secret_access_key: Option<String>,
        path_style_access: Option<bool>,
    },
}

#[derive(Debug)]
pub struct TestRemoteDatabaseConfig {
    pub host: String,
    pub port: u16,
    pub database: String,
    pub username: String,
    pub password: String,
}

#[derive(Debug)]
pub struct TestS3Config {
    pub bucket: String,
    pub region: String,
    pub endpoint_url: Option<String>,
    pub auth_method: String,
    pub access_key_id: Option<String>,
    pub secret_access_key: Option<String>,
    pub path_style_access: bool,
}

pub fn routes() -> Router {
    Router::new()
        .route(
            "/connections",
            get(list_connections).post(create_connection),
        )
        .route(
            "/connections/{name}",
            get(get_connection)
                .put(update_connection)
                .delete(delete_connection),
        )
        .route("/connections/test", post(test_connection))
}

fn encrypt_connection_secrets(
    mut connection: ConnectionConfig,
    project_dir: &StdPath,
) -> Result<ConnectionConfig, Error> {
    match &mut connection {
        ConnectionConfig::MySql(config) => {
            if let SecretField::PlainText { value } = &config.password {
                let key_path = project_dir.join(".secret.key");
                config.password = SecretField::encrypt(value, &key_path).map_err(|e| {
                    Error::internal_server_error()
                        .with_message(format!("Failed to encrypt password: {}", e))
                })?;
            }
        }
        ConnectionConfig::PostgreSql(config) => {
            if let SecretField::PlainText { value } = &config.password {
                let key_path = project_dir.join(".secret.key");
                config.password = SecretField::encrypt(value, &key_path).map_err(|e| {
                    Error::internal_server_error()
                        .with_message(format!("Failed to encrypt password: {}", e))
                })?;
            }
        }
        ConnectionConfig::S3(config) => {
            if let Some(SecretField::PlainText { value }) = &config.secret_access_key {
                let key_path = project_dir.join(".secret.key");
                config.secret_access_key =
                    Some(SecretField::encrypt(value, &key_path).map_err(|e| {
                        Error::internal_server_error()
                            .with_message(format!("Failed to encrypt secret access key: {}", e))
                    })?);
            }
        }
        _ => {}
    }
    Ok(connection)
}

fn decrypt_connection_secrets(
    mut connection: ConnectionConfig,
    project_dir: &StdPath,
) -> Result<ConnectionConfig, Error> {
    match &mut connection {
        ConnectionConfig::MySql(config) => {
            config.password.load(project_dir).map_err(|e| {
                Error::internal_server_error()
                    .with_message(format!("Failed to decrypt password: {}", e))
            })?;
        }
        ConnectionConfig::PostgreSql(config) => {
            config.password.load(project_dir).map_err(|e| {
                Error::internal_server_error()
                    .with_message(format!("Failed to decrypt password: {}", e))
            })?;
        }
        ConnectionConfig::S3(config) => {
            if let Some(ref mut secret) = config.secret_access_key {
                secret.load(project_dir)
            } else {
                Ok(())
            }
            .map_err(|e| {
                Error::internal_server_error()
                    .with_message(format!("Failed to decrypt secret access key: {}", e))
            })?;
        }
        _ => {}
    }
    Ok(connection)
}

async fn test_connection(Json(connection): Json<TestConnectionConfig>) -> Result<(), Error> {
    match connection {
        TestConnectionConfig::SQLite { path } => test_sqlite_connection(&path).await,
        TestConnectionConfig::LocalFile { base_path } => {
            test_localfile_connection(&base_path).await
        }
        TestConnectionConfig::MySQL {
            host,
            port,
            database,
            username,
            password,
        } => {
            let config = TestRemoteDatabaseConfig {
                host,
                port,
                database,
                username,
                password,
            };
            test_mysql_connection(&config).await
        }
        TestConnectionConfig::PostgreSQL {
            host,
            port,
            database,
            username,
            password,
        } => {
            let config = TestRemoteDatabaseConfig {
                host,
                port,
                database,
                username,
                password,
            };
            test_postgresql_connection(&config).await
        }
        TestConnectionConfig::S3 {
            bucket,
            region,
            endpoint_url,
            auth_method,
            access_key_id,
            secret_access_key,
            path_style_access,
        } => {
            let s3_config = TestS3Config {
                bucket,
                region,
                endpoint_url,
                auth_method,
                access_key_id,
                secret_access_key,
                path_style_access: path_style_access.unwrap_or(false),
            };
            test_s3_connection(&s3_config).await
        }
    }?;

    Ok(())
}

async fn test_localfile_connection(base_path: &str) -> Result<(), Error> {
    let path = StdPath::new(base_path);

    if !path.exists() {
        return Err(
            Error::bad_request().with_message(format!("Path does not exist: {}", base_path))
        );
    }

    if path.is_dir() {
        match tokio::fs::read_dir(base_path).await {
            Ok(_) => Ok(()),
            Err(e) => {
                Err(Error::bad_request().with_message(format!("Cannot read directory: {}", e)))
            }
        }
    } else {
        match tokio::fs::File::open(base_path).await {
            Ok(_) => Ok(()),
            Err(e) => Err(Error::bad_request().with_message(format!("Cannot read file: {}", e))),
        }
    }
}

async fn test_s3_connection(s3_config: &TestS3Config) -> Result<(), Error> {
    use aws_config::Region;
    use aws_sdk_s3::{
        Client,
        config::{Builder, Credentials},
    };

    let mut config_loader = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(Region::new(s3_config.region.clone()));

    if let Some(endpoint) = &s3_config.endpoint_url {
        config_loader = config_loader.endpoint_url(endpoint);
    }

    if s3_config.auth_method == "explicit"
        && let (Some(access_key), Some(secret_key)) =
            (&s3_config.access_key_id, &s3_config.secret_access_key)
    {
        let credentials = Credentials::new(access_key, secret_key, None, None, "test");
        config_loader = config_loader.credentials_provider(credentials);
    }

    let aws_config = config_loader.load().await;
    let s3_config_builder = Builder::from(&aws_config);
    let aws_s3_config = if s3_config.path_style_access {
        s3_config_builder.force_path_style(true).build()
    } else {
        s3_config_builder.build()
    };

    let client = Client::from_conf(aws_s3_config);

    match client.head_bucket().bucket(&s3_config.bucket).send().await {
        Ok(_) => Ok(()),
        Err(e) => Err(Error::bad_request().with_message(format!(
            "Failed to access S3 bucket '{}': {}",
            s3_config.bucket, e
        ))),
    }
}

async fn test_sqlite_connection(path: &str) -> Result<(), Error> {
    let sqlite_path = StdPath::new(path);

    if !sqlite_path.exists() {
        return Err(Error::bad_request()
            .with_message(format!("SQLite database file does not exist: {}", path)));
    }

    match rusqlite::Connection::open(path) {
        Ok(conn) => match conn.prepare("SELECT 1") {
            Ok(mut stmt) => match stmt.query([]) {
                Ok(_) => Ok(()),
                Err(e) => Err(Error::bad_request()
                    .with_message(format!("SQLite connection test query failed: {}", e))),
            },
            Err(e) => Err(Error::bad_request().with_message(format!(
                "SQLite connection test query prepare failed: {}",
                e
            ))),
        },
        Err(e) => Err(Error::bad_request()
            .with_message(format!("Failed to connect to SQLite database: {}", e))),
    }
}

async fn test_mysql_connection(config: &TestRemoteDatabaseConfig) -> Result<(), Error> {
    use mysql::prelude::*;

    let url = format!(
        "mysql://{}:{}@{}:{}/{}",
        config.username, config.password, config.host, config.port, config.database
    );

    match mysql::Pool::new(url.as_str()) {
        Ok(pool) => {
            match pool.get_conn() {
                Ok(mut conn) => match conn.query_drop("SELECT 1") {
                    Ok(_) => Ok(()),
                    Err(e) => Err(Error::bad_request()
                        .with_message(format!("MySQL connection test query failed: {}", e))),
                },
                Err(e) => Err(Error::bad_request()
                    .with_message(format!("Failed to get MySQL connection: {}", e))),
            }
        }
        Err(e) => Err(Error::bad_request()
            .with_message(format!("Failed to connect to MySQL database: {}", e))),
    }
}

async fn test_postgresql_connection(config: &TestRemoteDatabaseConfig) -> Result<(), Error> {
    let connection_string = format!(
        "host={} port={} dbname={} user={} password={}",
        config.host, config.port, config.database, config.username, config.password
    );

    match tokio_postgres::connect(&connection_string, tokio_postgres::NoTls).await {
        Ok((client, connection)) => {
            let handle = tokio::spawn(connection);

            match client.query("SELECT 1", &[]).await {
                Ok(_) => {
                    handle.abort();
                    Ok(())
                }
                Err(e) => {
                    handle.abort();
                    Err(Error::bad_request()
                        .with_message(format!("PostgreSQL connection test query failed: {}", e)))
                }
            }
        }
        Err(e) => Err(Error::bad_request()
            .with_message(format!("Failed to connect to PostgreSQL database: {}", e))),
    }
}

async fn list_connections(
    Extension(config): Extension<Arc<Mutex<Config>>>,
) -> Result<Json<Vec<ConnectionSummary>>, Error> {
    let config = config.lock().await;
    let mut connections = Vec::new();
    for (name, conn_config) in &config.project.connections {
        let (connection_type, details) = match conn_config {
            ConnectionConfig::LocalFile { base_path } => {
                ("localfile".to_string(), base_path.clone())
            }
            ConnectionConfig::Sqlite { path } => ("sqlite".to_string(), path.clone()),
            ConnectionConfig::MySql(mysql_config) => match mysql_config.password.plaintext() {
                Ok(_) => (
                    "mysql".to_string(),
                    format!("{}@{}", mysql_config.database, mysql_config.host),
                ),
                Err(e) => {
                    return Err(Error::internal_server_error().with_message(format!(
                        "Failed to access MySQL password for connection '{}': {}",
                        name, e
                    )));
                }
            },
            ConnectionConfig::PostgreSql(pg_config) => match pg_config.password.plaintext() {
                Ok(_) => (
                    "postgresql".to_string(),
                    format!("{}@{}", pg_config.database, pg_config.host),
                ),
                Err(e) => {
                    return Err(Error::internal_server_error().with_message(format!(
                        "Failed to access PostgreSQL password for connection '{}': {}",
                        name, e
                    )));
                }
            },
            ConnectionConfig::S3(s3_config) => {
                if let Some(ref secret_key) = s3_config.secret_access_key {
                    match secret_key.plaintext() {
                        Ok(_) => {}
                        Err(e) => {
                            return Err(Error::internal_server_error().with_message(format!(
                                "Failed to access S3 secret key for connection '{}': {}",
                                name, e
                            )));
                        }
                    }
                }
                ("s3".to_string(), s3_config.bucket.clone())
            }
        };

        let summary = ConnectionSummary {
            name: name.clone(),
            connection_type,
            details,
        };

        connections.push(summary);
    }

    connections.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(Json(connections))
}

async fn get_connection(
    Extension(config): Extension<Arc<Mutex<Config>>>,
    Path(name): Path<String>,
) -> Result<Json<ConnectionConfig>, Error> {
    let config = config.lock().await;
    match config.project.connections.get(&name) {
        Some(conn_config) => Ok(Json(conn_config.clone())),
        None => Error::not_found().build(),
    }
}

async fn create_connection(
    Extension(config): Extension<Arc<Mutex<Config>>>,
    Json(req): Json<CreateConnectionRequest>,
) -> Result<(), Error> {
    let mut config = config.lock().await;
    let mut project_config = config.project.clone();

    if project_config.connections.contains_key(&req.name) {
        return Error::conflict().build();
    }

    let encrypted_config = encrypt_connection_secrets(req.config.clone(), &config.project_dir)?;
    project_config
        .connections
        .insert(req.name.clone(), encrypted_config);
    let project_file = config.add_project_setting(&project_config)?;
    project_file.save()?;

    let decrypted_config = decrypt_connection_secrets(req.config, &config.project_dir)?;
    project_config
        .connections
        .insert(req.name.clone(), decrypted_config);
    config.add_project_setting(&project_config)?;

    Ok(())
}

async fn update_connection(
    Extension(config): Extension<Arc<Mutex<Config>>>,
    Path(name): Path<String>,
    Json(connection): Json<ConnectionConfig>,
) -> Result<(), Error> {
    let mut config = config.lock().await;
    let mut project_config = config.project.clone();

    if !project_config.connections.contains_key(&name) {
        return Error::not_found().build();
    }

    let encrypted_connection = encrypt_connection_secrets(connection.clone(), &config.project_dir)?;
    project_config
        .connections
        .insert(name.clone(), encrypted_connection);
    let project_file = config.add_project_setting(&project_config)?;
    project_file.save()?;

    let decrypted_connection = decrypt_connection_secrets(connection, &config.project_dir)?;
    project_config
        .connections
        .insert(name, decrypted_connection);
    config.add_project_setting(&project_config)?;

    Ok(())
}

async fn delete_connection(
    Extension(config): Extension<Arc<Mutex<Config>>>,
    Path(name): Path<String>,
) -> Result<(), Error> {
    let mut config = config.lock().await;
    let mut project_config = config.project.clone();

    if !project_config.connections.contains_key(&name) {
        return Error::not_found().build();
    }

    project_config.connections.remove(&name);
    let project_file = config.add_project_setting(&project_config)?;
    project_file.save()?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        api::StatusCode,
        core::{
            adapter::test_helpers::{
                create_test_s3_config, setup_minio_test_data, test_encrypted_field,
            },
            config::project::RemoteDatabaseConfig,
        },
        test_helpers::TestManager,
    };
    use anyhow::Result;
    use serde_json::json;

    #[tokio::test]
    async fn test_create_connection_localfile() -> Result<()> {
        let test = TestManager::new();
        let server = test.setup_server(routes);

        let new_connection = json!({
            "name": "test_local_connection",
            "config": {
                "type": "localfile",
                "config": {
                    "base_path": "/tmp/test_data"
                }
            }
        });

        let response = server.post("/connections").json(&new_connection).await;
        response.assert_status_ok();

        let get_response = server.get("/connections/test_local_connection").await;
        get_response.assert_status_ok();

        let connection_config: ConnectionConfig = get_response.json();
        match connection_config {
            ConnectionConfig::LocalFile { base_path } => {
                assert_eq!(base_path, "/tmp/test_data");
            }
            _ => panic!("Expected LocalFile connection"),
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_create_connection_sqlite() -> Result<()> {
        let test = TestManager::new();
        let server = test.setup_server(routes);

        let new_connection = json!({
            "name": "test_sqlite_connection",
            "config": {
                "type": "sqlite",
                "config": {
                    "path": "/tmp/test.db"
                }
            }
        });

        let response = server.post("/connections").json(&new_connection).await;
        response.assert_status_ok();

        let get_response = server.get("/connections/test_sqlite_connection").await;
        get_response.assert_status_ok();

        let connection_config: ConnectionConfig = get_response.json();
        match connection_config {
            ConnectionConfig::Sqlite { path } => {
                assert_eq!(path, "/tmp/test.db");
            }
            _ => panic!("Expected Sqlite connection"),
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_create_connection_mysql() -> Result<()> {
        let test = TestManager::new();
        let server = test.setup_server(routes);

        let new_connection = json!({
            "name": "test_mysql_connection",
            "config": {
                "type": "mysql",
                "config": {
                    "host": "localhost",
                    "port": 3306,
                    "database": "testdb",
                    "username": "user",
                    "password": {
                        "type": "plain",
                        "value": "pass"
                    }
                }
            }
        });

        let response = server.post("/connections").json(&new_connection).await;
        response.assert_status_ok();

        let get_response = server.get("/connections/test_mysql_connection").await;
        get_response.assert_status_ok();

        let connection_config: ConnectionConfig = get_response.json();
        match connection_config {
            ConnectionConfig::MySql(config) => {
                assert_eq!(config.host, "localhost");
                assert_eq!(config.port, 3306);
                assert_eq!(config.database, "testdb");
                assert_eq!(config.username, "user");
            }
            _ => panic!("Expected MySql connection"),
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_update_connection() -> Result<()> {
        let test = TestManager::new();
        let server = test.setup_server(routes);

        let original_connection = ConnectionConfig::LocalFile {
            base_path: "/original/path".to_string(),
        };

        {
            let mut config = test.config().await;
            let mut project_config = config.project.clone();
            project_config
                .connections
                .insert("test_connection".to_string(), original_connection);
            let project_file = config.add_project_setting(&project_config)?;
            project_file.save()?;
        }

        let updated_config = json!({
            "type": "sqlite",
            "config": {
                "path": "/updated/database.db"
            }
        });

        let response = server
            .put("/connections/test_connection")
            .json(&updated_config)
            .await;
        response.assert_status_ok();

        let get_response = server.get("/connections/test_connection").await;
        get_response.assert_status_ok();

        let connection_config: ConnectionConfig = get_response.json();
        match connection_config {
            ConnectionConfig::Sqlite { path } => {
                assert_eq!(path, "/updated/database.db");
            }
            _ => panic!("Expected Sqlite connection"),
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_delete_connection() -> Result<()> {
        let test = TestManager::new();
        let server = test.setup_server(routes);

        let connection_config = ConnectionConfig::LocalFile {
            base_path: "/tmp/test".to_string(),
        };

        {
            let mut config = test.config().await;
            let mut project_config = config.project.clone();
            project_config
                .connections
                .insert("connection_to_delete".to_string(), connection_config);
            let project_file = config.add_project_setting(&project_config)?;
            project_file.save()?;
        }

        let get_response = server.get("/connections/connection_to_delete").await;
        get_response.assert_status_ok();

        let delete_response = server.delete("/connections/connection_to_delete").await;
        delete_response.assert_status_ok();

        let get_response_after = server.get("/connections/connection_to_delete").await;
        get_response_after.assert_status(StatusCode::NOT_FOUND);

        Ok(())
    }

    #[tokio::test]
    async fn test_list_connections() -> Result<()> {
        let test = TestManager::new();
        let server = test.setup_server(routes);

        {
            let mut config = test.config().await;
            let mut project_config = config.project.clone();

            project_config.connections.insert(
                "local_conn".to_string(),
                ConnectionConfig::LocalFile {
                    base_path: "/tmp/data".to_string(),
                },
            );

            project_config.connections.insert(
                "sqlite_conn".to_string(),
                ConnectionConfig::Sqlite {
                    path: "/tmp/db.sqlite".to_string(),
                },
            );

            let project_file = config.add_project_setting(&project_config)?;
            project_file.save()?;
        }

        let response = server.get("/connections").await;
        response.assert_status_ok();

        let connections: Vec<ConnectionSummary> = response.json();
        assert_eq!(connections.len(), 2);

        let local_conn = connections.iter().find(|c| c.name == "local_conn").unwrap();
        assert_eq!(local_conn.connection_type, "localfile");
        assert_eq!(local_conn.details, "/tmp/data");

        let sqlite_conn = connections
            .iter()
            .find(|c| c.name == "sqlite_conn")
            .unwrap();
        assert_eq!(sqlite_conn.connection_type, "sqlite");
        assert_eq!(sqlite_conn.details, "/tmp/db.sqlite");

        Ok(())
    }

    #[tokio::test]
    async fn test_create_connection_conflict() -> Result<()> {
        let test = TestManager::new();
        let server = test.setup_server(routes);

        let connection_config = ConnectionConfig::LocalFile {
            base_path: "/tmp/existing".to_string(),
        };

        {
            let mut config = test.config().await;
            let mut project_config = config.project.clone();
            project_config
                .connections
                .insert("existing_connection".to_string(), connection_config);
            let project_file = config.add_project_setting(&project_config)?;
            project_file.save()?;
        }

        let duplicate_connection = json!({
            "name": "existing_connection",
            "config": {
                "type": "localfile",
                "config": {
                    "base_path": "/tmp/duplicate"
                }
            }
        });

        let response = server
            .post("/connections")
            .json(&duplicate_connection)
            .await;
        response.assert_status(StatusCode::CONFLICT);

        Ok(())
    }

    #[tokio::test]
    async fn test_update_nonexistent_connection() -> Result<()> {
        let test = TestManager::new();
        let server = test.setup_server(routes);

        let updated_config = json!({
            "type": "sqlite",
            "config": {
                "path": "/tmp/nonexistent.db"
            }
        });

        let response = server
            .put("/connections/nonexistent_connection")
            .json(&updated_config)
            .await;
        response.assert_status(StatusCode::NOT_FOUND);

        Ok(())
    }

    #[tokio::test]
    async fn test_delete_nonexistent_connection() -> Result<()> {
        let test = TestManager::new();
        let server = test.setup_server(routes);

        let response = server.delete("/connections/nonexistent_connection").await;
        response.assert_status(StatusCode::NOT_FOUND);

        Ok(())
    }

    #[tokio::test]
    async fn test_connection_localfile_success() -> Result<()> {
        let test = TestManager::new();
        let server = test.setup_server(routes);

        let temp_dir = tempfile::tempdir().unwrap();
        let test_path = temp_dir.path().to_string_lossy().to_string();

        let connection_config = json!({
            "type": "localfile",
            "base_path": test_path
        });

        let response = server
            .post("/connections/test")
            .json(&connection_config)
            .await;
        response.assert_status_ok();

        Ok(())
    }

    #[tokio::test]
    async fn test_connection_localfile_path_not_found() -> Result<()> {
        let test = TestManager::new();
        let server = test.setup_server(routes);

        let connection_config = json!({
            "type": "localfile",
            "base_path": "/non/existent/path"
        });

        let response = server
            .post("/connections/test")
            .json(&connection_config)
            .await;
        response.assert_status(StatusCode::BAD_REQUEST);

        let body: serde_json::Value = response.json();
        assert!(
            body["message"]
                .as_str()
                .unwrap()
                .contains("Path does not exist")
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_connection_sqlite_success() -> Result<()> {
        let test = TestManager::new();
        let server = test.setup_server(routes);

        let temp_dir = tempfile::tempdir().unwrap();
        let db_path = temp_dir.path().join("test.db");

        let connection = rusqlite::Connection::open(&db_path)?;
        connection.execute("CREATE TABLE test_table (id INTEGER, name TEXT)", [])?;

        let connection_config = json!({
            "type": "sqlite",
            "path": db_path.to_string_lossy()
        });

        let response = server
            .post("/connections/test")
            .json(&connection_config)
            .await;
        response.assert_status_ok();

        Ok(())
    }

    #[tokio::test]
    async fn test_connection_sqlite_path_not_found() -> Result<()> {
        let test = TestManager::new();
        let server = test.setup_server(routes);

        let connection_config = json!({
            "type": "sqlite",
            "path": "/non/existent/database.db"
        });

        let response = server
            .post("/connections/test")
            .json(&connection_config)
            .await;
        response.assert_status(StatusCode::BAD_REQUEST);

        let body: serde_json::Value = response.json();
        assert!(body["message"].as_str().unwrap().contains("does not exist"));

        Ok(())
    }

    #[tokio::test]
    async fn test_connection_mysql_success() -> Result<()> {
        let connection_config = ConnectionConfig::MySql(RemoteDatabaseConfig {
            host: "localhost".to_string(),
            port: 3307,
            database: "datasource_test".to_string(),
            username: "datasource".to_string(),
            password: test_encrypted_field("datasourcepass"),
        });

        let result = match connection_config {
            ConnectionConfig::MySql(config) => {
                let test_config = TestRemoteDatabaseConfig {
                    host: config.host.clone(),
                    port: config.port,
                    database: config.database.clone(),
                    username: config.username.clone(),
                    password: "datasourcepass".to_string(),
                };
                test_mysql_connection(&test_config).await
            }
            _ => panic!("Expected MySQL connection"),
        };

        assert!(result.is_ok());
        Ok(())
    }

    #[tokio::test]
    async fn test_connection_mysql_invalid_password() -> Result<()> {
        let test = TestManager::new();
        let server = test.setup_server(routes);

        let connection_config = TestConnectionConfig::MySQL {
            host: "localhost".to_string(),
            port: 3307,
            database: "datasource_test".to_string(),
            username: "datasource".to_string(),
            password: "invalidpass".to_string(),
        };

        let response = server
            .post("/connections/test")
            .json(&connection_config)
            .await;
        response.assert_status(StatusCode::BAD_REQUEST);

        let body: serde_json::Value = response.json();
        assert!(
            body["message"].as_str().unwrap().contains("MySQL")
                || body["message"].as_str().unwrap().contains("connection")
                || body["message"].as_str().unwrap().contains("password")
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_connection_postgresql_success() -> Result<()> {
        let connection_config = ConnectionConfig::PostgreSql(RemoteDatabaseConfig {
            host: "localhost".to_string(),
            port: 5433,
            database: "datasource_test".to_string(),
            username: "datasource".to_string(),
            password: test_encrypted_field("datasourcepass"),
        });

        let result = match connection_config {
            ConnectionConfig::PostgreSql(config) => {
                let test_config = TestRemoteDatabaseConfig {
                    host: config.host.clone(),
                    port: config.port,
                    database: config.database.clone(),
                    username: config.username.clone(),
                    password: "datasourcepass".to_string(),
                };
                test_postgresql_connection(&test_config).await
            }
            _ => panic!("Expected PostgreSQL connection"),
        };

        assert!(result.is_ok());
        Ok(())
    }

    #[tokio::test]
    async fn test_connection_postgresql_database_not_found() -> Result<()> {
        let test = TestManager::new();
        let server = test.setup_server(routes);

        let connection_config = TestConnectionConfig::PostgreSQL {
            host: "localhost".to_string(),
            port: 5433,
            database: "nonexistent_database".to_string(),
            username: "datasource".to_string(),
            password: "datasourcepass".to_string(),
        };

        let response = server
            .post("/connections/test")
            .json(&connection_config)
            .await;
        response.assert_status(StatusCode::BAD_REQUEST);

        let body: serde_json::Value = response.json();
        assert!(
            body["message"].as_str().unwrap().contains("PostgreSQL")
                || body["message"].as_str().unwrap().contains("database")
                || body["message"].as_str().unwrap().contains("connection")
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_connection_s3_success() -> Result<()> {
        setup_minio_test_data().await?;

        let s3_config = create_test_s3_config();
        let connection_config = ConnectionConfig::S3(s3_config);

        let result = match connection_config {
            ConnectionConfig::S3(config) => {
                let test_config = TestS3Config {
                    bucket: config.bucket.clone(),
                    region: config.region.clone(),
                    endpoint_url: config.endpoint_url.clone(),
                    auth_method: "explicit".to_string(),
                    access_key_id: config.access_key_id.clone(),
                    secret_access_key: Some("password".to_string()),
                    path_style_access: config.path_style_access,
                };
                test_s3_connection(&test_config).await
            }
            _ => panic!("Expected S3 connection"),
        };

        match result {
            Ok(_) => Ok(()),
            Err(e) => Err(anyhow::anyhow!("S3 connection test failed: {:?}", e)),
        }
    }

    #[tokio::test]
    async fn test_connection_s3_bucket_not_found() -> Result<()> {
        let test = TestManager::new();
        let server = test.setup_server(routes);

        let connection_config = TestConnectionConfig::S3 {
            bucket: "nonexistent-bucket".to_string(),
            region: "us-east-1".to_string(),
            endpoint_url: Some("http://localhost:9010".to_string()),
            auth_method: "explicit".to_string(),
            access_key_id: Some("user".to_string()),
            secret_access_key: Some("password".to_string()),
            path_style_access: Some(true),
        };

        let response = server
            .post("/connections/test")
            .json(&connection_config)
            .await;
        response.assert_status(StatusCode::BAD_REQUEST);

        let body: serde_json::Value = response.json();
        assert!(
            body["message"].as_str().unwrap().contains("S3")
                || body["message"].as_str().unwrap().contains("bucket")
        );

        Ok(())
    }
}
