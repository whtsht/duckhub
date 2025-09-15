use super::secret::SecretField;
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectConfig {
    pub storage: StorageConfig,
    pub database: DatabaseConfig,
    pub connections: HashMap<String, ConnectionConfig>,
}

impl ProjectConfig {
    pub fn validate(&self, project_dir: &std::path::Path) -> Result<Vec<String>> {
        let mut warnings = Vec::new();

        if !project_dir.join(".secret.key").exists() {
            let has_encrypted_fields = self.connections.values().any(|conn| match conn {
                ConnectionConfig::MySql(config) => {
                    matches!(config.password, SecretField::Encrypted { .. })
                }
                ConnectionConfig::PostgreSql(config) => {
                    matches!(config.password, SecretField::Encrypted { .. })
                }
                ConnectionConfig::S3(config) => config
                    .secret_access_key
                    .as_ref()
                    .is_some_and(|key| matches!(key, SecretField::Encrypted { .. })),
                _ => false,
            }) || matches!(self.storage, StorageConfig::S3(ref config) if config.secret_access_key.as_ref().is_some_and(|key| matches!(key, SecretField::Encrypted { .. })));

            if has_encrypted_fields {
                warnings.push("Encrypted fields found but .secret.key file is missing. Encrypted fields will fail to decrypt.".to_string());
            }
        }

        for (name, connection) in &self.connections {
            match connection {
                ConnectionConfig::LocalFile { base_path } => {
                    let resolved_path = if std::path::Path::new(base_path).is_absolute() {
                        std::path::PathBuf::from(base_path)
                    } else {
                        project_dir.join(base_path.trim_start_matches("./"))
                    };

                    if !resolved_path.exists() {
                        warnings.push(format!(
                            "Connection '{}': Local file path '{}' does not exist",
                            name,
                            resolved_path.display()
                        ));
                    }
                }
                ConnectionConfig::Sqlite { path } => {
                    let resolved_path = if std::path::Path::new(path).is_absolute() {
                        std::path::PathBuf::from(path)
                    } else {
                        project_dir.join(path.trim_start_matches("./"))
                    };

                    if let Some(parent) = resolved_path.parent()
                        && !parent.exists()
                    {
                        warnings.push(format!(
                            "Connection '{}': SQLite database directory '{}' does not exist",
                            name,
                            parent.display()
                        ));
                    }
                }
                ConnectionConfig::MySql(config) => {
                    if config.host.is_empty() {
                        warnings.push(format!("Connection '{}': MySQL host is empty", name));
                    }
                    if config.database.is_empty() {
                        warnings.push(format!(
                            "Connection '{}': MySQL database name is empty",
                            name
                        ));
                    }
                    if config.username.is_empty() {
                        warnings.push(format!("Connection '{}': MySQL username is empty", name));
                    }
                }
                ConnectionConfig::PostgreSql(config) => {
                    if config.host.is_empty() {
                        warnings.push(format!("Connection '{}': PostgreSQL host is empty", name));
                    }
                    if config.database.is_empty() {
                        warnings.push(format!(
                            "Connection '{}': PostgreSQL database name is empty",
                            name
                        ));
                    }
                    if config.username.is_empty() {
                        warnings.push(format!(
                            "Connection '{}': PostgreSQL username is empty",
                            name
                        ));
                    }
                }
                ConnectionConfig::S3(config) => {
                    if config.bucket.is_empty() {
                        warnings.push(format!("Connection '{}': S3 bucket name is empty", name));
                    }
                    if config.region.is_empty() {
                        warnings.push(format!("Connection '{}': S3 region is empty", name));
                    }
                    if matches!(config.auth_method, S3AuthMethod::Explicit) {
                        if config.access_key_id.is_none() {
                            warnings.push(format!(
                                "Connection '{}': S3 access key ID is required for explicit auth",
                                name
                            ));
                        }
                        if config.secret_access_key.is_none() {
                            warnings.push(format!("Connection '{}': S3 secret access key is required for explicit auth", name));
                        }
                    }
                }
            }
        }

        Ok(warnings)
    }
}

impl ProjectConfig {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn resolve_paths(&mut self, project_dir: &std::path::Path) -> Result<()> {
        match &mut self.storage {
            StorageConfig::LocalFile { path } => {
                if !std::path::Path::new(path).is_absolute() {
                    *path = project_dir
                        .join(path.trim_start_matches("./"))
                        .to_string_lossy()
                        .to_string();
                }
            }
            StorageConfig::S3(_) => {}
        }

        if let Some(db_path) = &mut self.database.path
            && !std::path::Path::new(db_path).is_absolute()
        {
            *db_path = project_dir
                .join(db_path.trim_start_matches("./"))
                .to_string_lossy()
                .to_string();
        }

        for connection in self.connections.values_mut() {
            match connection {
                ConnectionConfig::LocalFile { base_path } => {
                    if !std::path::Path::new(base_path).is_absolute() {
                        *base_path = project_dir
                            .join(base_path.trim_start_matches("./"))
                            .to_string_lossy()
                            .to_string();
                    }
                }
                ConnectionConfig::Sqlite { path } => {
                    if !std::path::Path::new(path).is_absolute() {
                        *path = project_dir
                            .join(path.trim_start_matches("./"))
                            .to_string_lossy()
                            .to_string();
                    }
                }
                _ => {}
            }
        }

        Ok(())
    }

    pub fn load_secrets(&mut self, project_dir: &std::path::Path) -> Result<()> {
        if let Some(password) = &mut self.database.password {
            password
                .load(project_dir)
                .with_context(|| "Failed to load database password")?;
        }

        for (connection_name, connection) in self.connections.iter_mut() {
            match connection {
                ConnectionConfig::MySql(config) => {
                    config.password.load(project_dir).with_context(|| {
                        format!(
                            "Failed to load MySQL password for connection '{}'",
                            connection_name
                        )
                    })?;
                }
                ConnectionConfig::PostgreSql(config) => {
                    config.password.load(project_dir).with_context(|| {
                        format!(
                            "Failed to load PostgreSQL password for connection '{}'",
                            connection_name
                        )
                    })?;
                }
                ConnectionConfig::S3(config) => {
                    if let Some(secret_key) = &mut config.secret_access_key {
                        secret_key.load(project_dir).with_context(|| {
                            format!(
                                "Failed to load S3 secret access key for connection '{}'",
                                connection_name
                            )
                        })?;
                    }
                }
                _ => {}
            }
        }

        if let StorageConfig::S3(s3_config) = &mut self.storage
            && let Some(secret_key) = &mut s3_config.secret_access_key
        {
            secret_key
                .load(project_dir)
                .with_context(|| "Failed to load S3 storage secret access key")?;
        }

        Ok(())
    }
}

impl Default for ProjectConfig {
    fn default() -> Self {
        Self {
            storage: StorageConfig::LocalFile {
                path: "./storage".to_string(),
            },
            database: DatabaseConfig {
                ty: DatabaseType::Sqlite,
                path: Some("./database.db".to_string()),
                host: None,
                port: None,
                database: None,
                username: None,
                password: None,
            },
            connections: HashMap::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", content = "config")]
pub enum StorageConfig {
    #[serde(rename = "local")]
    LocalFile { path: String },
    #[serde(rename = "s3")]
    S3(S3Config),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct S3Config {
    pub bucket: String,
    pub region: String,
    pub endpoint_url: Option<String>,
    pub auth_method: S3AuthMethod,
    pub access_key_id: Option<String>,
    pub secret_access_key: Option<SecretField>,
    pub path_style_access: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DatabaseConfig {
    #[serde(rename = "type")]
    pub ty: DatabaseType,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub host: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub port: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub database: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub username: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub password: Option<SecretField>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum S3AuthMethod {
    #[serde(rename = "credential_chain")]
    CredentialChain,
    #[default]
    #[serde(rename = "explicit")]
    Explicit,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RemoteDatabaseConfig {
    pub host: String,
    pub port: u16,
    pub database: String,
    pub username: String,
    pub password: SecretField,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DatabaseType {
    #[serde(rename = "sqlite")]
    Sqlite,
    #[serde(rename = "mysql")]
    Mysql,
    #[serde(rename = "postgresql")]
    Postgresql,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", content = "config")]
pub enum ConnectionConfig {
    #[serde(rename = "localfile")]
    LocalFile { base_path: String },
    #[serde(rename = "s3")]
    S3(S3Config),
    #[serde(rename = "sqlite")]
    Sqlite { path: String },
    #[serde(rename = "mysql")]
    MySql(RemoteDatabaseConfig),
    #[serde(rename = "postgresql")]
    PostgreSql(RemoteDatabaseConfig),
}

fn expand_env_vars(value: &str) -> Result<String, anyhow::Error> {
    let re = regex::Regex::new(r"\$\{([^}]+)\}").unwrap();
    let mut result = value.to_string();

    for cap in re.captures_iter(value) {
        let env_var_with_braces = &cap[0];
        let env_var = &cap[1];

        match std::env::var(env_var) {
            Ok(env_value) => {
                result = result.replace(env_var_with_braces, &env_value);
            }
            Err(_) => {
                return Err(anyhow::anyhow!(
                    "Environment variable '{}' not found or not accessible",
                    env_var
                ));
            }
        }
    }

    if result.contains("${") {
        return Err(anyhow::anyhow!(
            "Unclosed environment variable reference in: {}",
            value
        ));
    }

    Ok(result)
}

pub fn parse_project_config(yaml_str: &str) -> anyhow::Result<ProjectConfig> {
    let expanded_yaml = expand_env_vars(yaml_str)?;
    serde_yml::from_str(&expanded_yaml)
        .map_err(|e| anyhow::anyhow!("Failed to parse project config: {}", e))
}
