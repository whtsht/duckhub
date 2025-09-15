use crate::core::{
    config::{
        adapter::{AdapterConfig, AdapterSource, FileConfig, FormatConfig},
        project::{S3AuthMethod, S3Config, StorageConfig},
        secret::SecretField,
    },
    ducklake::{CatalogConfig, DuckLake},
};
use anyhow::Result;

use std::{fs, path::Path, sync::Arc};

pub async fn setup_test_ducklake(tempdir: &Path) -> Result<Arc<DuckLake>> {
    let catalog_config = CatalogConfig::Sqlite {
        path: tempdir
            .join("test_catalog.sqlite")
            .to_string_lossy()
            .to_string(),
    };
    let storage_config = StorageConfig::LocalFile {
        path: tempdir.to_string_lossy().to_string(),
    };

    let ducklake = Arc::new(DuckLake::new(catalog_config, storage_config).await?);
    Ok(ducklake)
}

pub fn create_test_csv_data() -> String {
    "id,name,age\n1,Alice,25\n2,Bob,30\n3,Charlie,35".to_string()
}

pub fn create_test_json_data() -> String {
    r#"[
        {"id": 1, "name": "Alice", "age": 25},
        {"id": 2, "name": "Bob", "age": 30},
        {"id": 3, "name": "Charlie", "age": 35}
    ]"#
    .to_string()
}

pub fn create_csv_adapter_config() -> AdapterConfig {
    AdapterConfig {
        connection: "local".to_string(),
        description: None,
        source: AdapterSource::File {
            file: FileConfig {
                path: "test_data.csv".to_string(),
                compression: None,
                max_batch_size: None,
            },
            format: FormatConfig {
                ty: "csv".to_string(),
                delimiter: None,
                null_value: None,
                has_header: Some(true),
            },
        },
        columns: vec![],
    }
}

pub fn create_json_adapter_config() -> AdapterConfig {
    AdapterConfig {
        connection: "local".to_string(),
        description: None,
        source: AdapterSource::File {
            file: FileConfig {
                path: "test_data.json".to_string(),
                compression: None,
                max_batch_size: None,
            },
            format: FormatConfig {
                ty: "json".to_string(),
                delimiter: None,
                null_value: None,
                has_header: None,
            },
        },
        columns: vec![],
    }
}

pub fn create_s3_adapter_config() -> AdapterConfig {
    AdapterConfig {
        connection: "s3".to_string(),
        description: None,
        source: AdapterSource::File {
            file: FileConfig {
                path: "test-data/*.csv".to_string(),
                compression: None,
                max_batch_size: None,
            },
            format: FormatConfig {
                ty: "csv".to_string(),
                delimiter: None,
                null_value: None,
                has_header: Some(true),
            },
        },
        columns: vec![],
    }
}

pub fn create_test_s3_config() -> S3Config {
    S3Config {
        bucket: "test-bucket".to_string(),
        region: "us-east-1".to_string(),
        endpoint_url: Some("http://localhost:9010".to_string()),
        auth_method: S3AuthMethod::Explicit,
        access_key_id: Some("user".to_string()),
        secret_access_key: Some(test_encrypted_field("password")),

        path_style_access: true,
    }
}

pub async fn create_test_sqlite_db(path: &Path) -> Result<()> {
    let connection = rusqlite::Connection::open(path)?;
    connection.execute(
        "CREATE TABLE test_table (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)",
        [],
    )?;
    connection.execute(
        "INSERT INTO test_table (name, age) VALUES ('Alice', 25), ('Bob', 30), ('Charlie', 35)",
        [],
    )?;
    Ok(())
}

pub fn write_test_file(dir: &Path, filename: &str, content: &str) -> Result<()> {
    let file_path = dir.join(filename);
    fs::write(file_path, content)?;
    Ok(())
}

pub async fn setup_minio_test_data() -> Result<()> {
    use crate::core::config::project::S3AuthMethod;
    use aws_config::Region;
    use aws_sdk_s3::{
        Client,
        config::{Builder, Credentials},
    };

    let s3_config = create_test_s3_config();

    let mut config_loader = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(Region::new(s3_config.region.clone()));

    if let Some(endpoint) = &s3_config.endpoint_url {
        config_loader = config_loader.endpoint_url(endpoint);
    }

    match &s3_config.auth_method {
        S3AuthMethod::CredentialChain => {}
        S3AuthMethod::Explicit => {
            let secret_access_key = s3_config
                .secret_access_key
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("secret_access_key is required"))?
                .plaintext()
                .expect("Test encrypted field should be already decrypted");
            let session_token = None;
            config_loader = config_loader.credentials_provider(Credentials::new(
                s3_config
                    .access_key_id
                    .as_ref()
                    .ok_or_else(|| anyhow::anyhow!("access_key_id is required"))?,
                secret_access_key,
                session_token,
                None,
                "duckhub",
            ));
        }
    }

    let aws_config = config_loader.load().await;
    let s3_config_builder = Builder::from(&aws_config);
    let aws_s3_config = if s3_config.path_style_access {
        s3_config_builder.force_path_style(true).build()
    } else {
        s3_config_builder.build()
    };

    let client = Client::from_conf(aws_s3_config);

    let _ = client
        .create_bucket()
        .bucket(&s3_config.bucket)
        .send()
        .await;

    let test_objects = vec![
        ("test-data/file1.csv", "id,name\n1,Alice\n2,Bob"),
        ("test-data/file2.csv", "id,name\n3,Charlie\n4,David"),
    ];

    for (key, content) in test_objects {
        let result = client
            .put_object()
            .bucket(&s3_config.bucket)
            .key(key)
            .body(content.as_bytes().to_vec().into())
            .send()
            .await;
        if result.is_err() {
            eprintln!("Warning: Failed to upload test object: {key}");
        }
    }

    Ok(())
}

pub async fn cleanup_minio_test_data() -> Result<()> {
    use crate::core::config::project::S3AuthMethod;
    use aws_config::Region;
    use aws_sdk_s3::{
        Client,
        config::{Builder, Credentials},
    };

    let s3_config = create_test_s3_config();

    let mut config_loader = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(Region::new(s3_config.region.clone()));

    if let Some(endpoint) = &s3_config.endpoint_url {
        config_loader = config_loader.endpoint_url(endpoint);
    }

    match &s3_config.auth_method {
        S3AuthMethod::CredentialChain => {}
        S3AuthMethod::Explicit => {
            let secret_access_key = s3_config
                .secret_access_key
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("secret_access_key is required"))?
                .plaintext()
                .expect("Test encrypted field should be already decrypted");
            let session_token = None;
            config_loader = config_loader.credentials_provider(Credentials::new(
                s3_config
                    .access_key_id
                    .as_ref()
                    .ok_or_else(|| anyhow::anyhow!("access_key_id is required"))?,
                secret_access_key,
                session_token,
                None,
                "duckhub",
            ));
        }
    }

    let aws_config = config_loader.load().await;
    let s3_config_builder = Builder::from(&aws_config);
    let aws_s3_config = if s3_config.path_style_access {
        s3_config_builder.force_path_style(true).build()
    } else {
        s3_config_builder.build()
    };

    let client = Client::from_conf(aws_s3_config);

    let keys = [
        "test-data/file1.csv".to_string(),
        "test-data/file2.csv".to_string(),
    ];

    if !keys.is_empty() {
        let delete_objects: Vec<_> = keys
            .iter()
            .map(|key| {
                aws_sdk_s3::types::ObjectIdentifier::builder()
                    .key(key)
                    .build()
                    .unwrap()
            })
            .collect();

        let result = client
            .delete_objects()
            .bucket(&s3_config.bucket)
            .delete(
                aws_sdk_s3::types::Delete::builder()
                    .set_objects(Some(delete_objects))
                    .build()
                    .unwrap(),
            )
            .send()
            .await;
        if result.is_err() {
            eprintln!("Warning: Failed to cleanup test objects");
        }
    }

    Ok(())
}

pub async fn setup_mysql_test_data() -> Result<()> {
    use mysql::{prelude::*, *};

    let url = "mysql://datasource:datasourcepass@localhost:3307/datasource_test";
    let pool = Pool::new(url)?;
    let mut conn = pool.get_conn()?;

    conn.query_drop("DROP TABLE IF EXISTS test_table")?;
    conn.query_drop(
        "CREATE TABLE test_table (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(50), age INT)",
    )?;
    conn.query_drop(
        "INSERT INTO test_table (name, age) VALUES ('Alice', 25), ('Bob', 30), ('Charlie', 35)",
    )?;

    Ok(())
}

pub async fn setup_postgres_test_data() -> Result<()> {
    use tokio_postgres::NoTls;

    let (client, connection) = tokio_postgres::connect(
        "host=localhost port=5433 user=datasource password=datasourcepass dbname=datasource_test",
        NoTls,
    )
    .await?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("PostgreSQL connection error: {e}");
        }
    });

    client
        .execute("DROP TABLE IF EXISTS test_table", &[])
        .await?;
    client
        .execute(
            "CREATE TABLE test_table (id SERIAL PRIMARY KEY, name VARCHAR(50), age INT)",
            &[],
        )
        .await?;
    client
        .execute(
            "INSERT INTO test_table (name, age) VALUES ('Alice', 25), ('Bob', 30), ('Charlie', 35)",
            &[],
        )
        .await?;

    Ok(())
}

pub fn test_encrypted_field(plaintext: &str) -> SecretField {
    SecretField::PlainText {
        value: plaintext.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_setup_test_ducklake() {
        let tempdir = tempdir().unwrap();
        let ducklake = setup_test_ducklake(tempdir.path()).await.unwrap();

        let result = ducklake.query("SELECT 1 as test").unwrap();
        assert_eq!(result[0][0], "1");
    }

    #[test]
    fn test_create_test_csv_data() {
        let data = create_test_csv_data();
        assert!(data.contains("id,name,age"));
        assert!(data.contains("Alice"));
    }

    #[tokio::test]
    async fn test_create_test_sqlite_db() {
        let tempdir = tempdir().unwrap();
        let db_path = tempdir.path().join("test.db");

        create_test_sqlite_db(&db_path).await.unwrap();

        let connection = rusqlite::Connection::open(&db_path).unwrap();
        let mut stmt = connection
            .prepare("SELECT COUNT(*) FROM test_table")
            .unwrap();
        let count: i64 = stmt.query_row([], |row| row.get(0)).unwrap();
        assert_eq!(count, 3);
    }
}
