use crate::core::{
    config::{
        Config,
        adapter::{AdapterConfig, AdapterSource, FileConfig, FormatConfig},
        dashboard::{ChartConfig, ChartType, DashboardConfig},
        model::ModelConfig,
        project::ConnectionConfig,
        query::QueryConfig,
    },
    graph::{Graph, dependent_tables},
};
use anyhow::Result;
use rusqlite::Connection;
use std::path::Path;

pub async fn create_samples(config: &mut Config) -> Result<()> {
    config.load()?;
    create_sample_data(&config.project_dir)?;
    create_sample_connection(config)?;
    create_sample_adapters(config)?;
    create_sample_models(config)?;
    create_sample_queries(config)?;
    create_sample_dashboards(config)?;
    create_sample_graph(config).await?;
    Ok(())
}

fn create_sample_data(project_dir: &Path) -> Result<()> {
    use std::fs;

    let sample_dir = project_dir.join("sample_data");
    fs::create_dir_all(&sample_dir)?;

    let users_csv = sample_dir.join("users.csv");
    let users_content = include_str!("./samples/users.csv");
    fs::write(&users_csv, users_content)?;

    let app_log_1 = sample_dir.join("app_log_1.json");
    let log1_content = include_str!("./samples/app_log_1.json");
    fs::write(&app_log_1, log1_content)?;

    let app_log_2 = sample_dir.join("app_log_2.json");
    let log2_content = include_str!("./samples/app_log_2.json");
    fs::write(&app_log_2, log2_content)?;

    let app_log_3 = sample_dir.join("app_log_3.json");
    let log3_content = include_str!("./samples/app_log_3.json");
    fs::write(&app_log_3, log3_content)?;

    let db_path = sample_dir.join("app.db");
    let conn = Connection::open(&db_path)?;

    conn.execute(
        "CREATE TABLE products (
                product_id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                category TEXT NOT NULL,
                price REAL NOT NULL,
                stock INTEGER NOT NULL,
                created_at TEXT NOT NULL
            )",
        [],
    )?;

    conn.execute(
        "INSERT INTO products (product_id, name, category, price, stock, created_at) VALUES
            (1, 'Laptop Pro', 'Electronics', 1299.99, 50, '2024-01-01'),
            (2, 'Wireless Mouse', 'Electronics', 29.99, 200, '2024-01-02'),
            (3, 'Office Chair', 'Furniture', 399.99, 75, '2024-01-03'),
            (4, 'Standing Desk', 'Furniture', 599.99, 40, '2024-01-04'),
            (5, 'USB-C Hub', 'Electronics', 49.99, 150, '2024-01-05'),
            (6, 'Monitor 27inch', 'Electronics', 349.99, 80, '2024-01-06'),
            (7, 'Desk Lamp', 'Furniture', 79.99, 120, '2024-01-07'),
            (8, 'Keyboard Mechanical', 'Electronics', 149.99, 90, '2024-01-08'),
            (9, 'Webcam HD', 'Electronics', 89.99, 110, '2024-01-09'),
            (10, 'Notebook Set', 'Stationery', 19.99, 300, '2024-01-10')",
        [],
    )?;

    conn.execute(
        "CREATE TABLE orders (
                order_id INTEGER PRIMARY KEY,
                user_id INTEGER NOT NULL,
                product_id INTEGER NOT NULL,
                quantity INTEGER NOT NULL,
                total_amount REAL NOT NULL,
                order_date TEXT NOT NULL,
                status TEXT NOT NULL
            )",
        [],
    )?;

    conn.execute(
            "INSERT INTO orders (order_id, user_id, product_id, quantity, total_amount, order_date, status) VALUES
            (1, 1, 1, 1, 1299.99, '2024-03-01', 'completed'),
            (2, 2, 2, 2, 59.98, '2024-03-01', 'completed'),
            (3, 3, 5, 1, 49.99, '2024-03-02', 'processing'),
            (4, 5, 4, 1, 599.99, '2024-03-02', 'completed'),
            (5, 7, 6, 1, 349.99, '2024-03-03', 'shipped'),
            (6, 8, 3, 2, 799.98, '2024-03-03', 'completed'),
            (7, 1, 8, 1, 149.99, '2024-03-04', 'processing'),
            (8, 4, 7, 3, 239.97, '2024-03-04', 'completed'),
            (9, 6, 9, 1, 89.99, '2024-03-05', 'shipped'),
            (10, 10, 10, 5, 99.95, '2024-03-05', 'completed')",
            [],
        )?;

    Ok(())
}

fn create_sample_connection(config: &mut Config) -> Result<()> {
    config.project.connections.insert(
        "local_files".to_string(),
        ConnectionConfig::LocalFile {
            base_path: "./sample_data".to_string(),
        },
    );

    config.project.connections.insert(
        "sample_db".to_string(),
        ConnectionConfig::Sqlite {
            path: "./sample_data/app.db".to_string(),
        },
    );

    config
        .add_project_setting(&config.project.clone())?
        .save()?;

    Ok(())
}

fn create_sample_adapters(config: &mut Config) -> Result<()> {
    // Users CSV adapter
    let users_config = AdapterConfig {
        connection: "local_files".to_string(),
        description: Some("User data from CSV file".to_string()),
        source: AdapterSource::File {
            file: FileConfig {
                path: "users.csv".to_string(),
                compression: None,
                max_batch_size: None,
            },
            format: FormatConfig {
                ty: "csv".to_string(),
                delimiter: Some(",".to_string()),
                null_value: None,
                has_header: Some(true),
            },
        },
        columns: vec![],
    };
    config.upsert_adapter("users", &users_config)?.save()?;

    // App logs JSON adapter
    let app_logs_config = AdapterConfig {
        connection: "local_files".to_string(),
        description: Some("Application logs from JSON files".to_string()),
        source: AdapterSource::File {
            file: FileConfig {
                path: "app_log_*.json".to_string(),
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
    };
    config
        .upsert_adapter("app_logs", &app_logs_config)?
        .save()?;

    // Products database adapter
    let products_config = AdapterConfig {
        connection: "sample_db".to_string(),
        description: Some("Product data from database".to_string()),
        source: AdapterSource::Database {
            table_name: "products".to_string(),
        },
        columns: vec![],
    };
    config
        .upsert_adapter("products", &products_config)?
        .save()?;

    // Orders database adapter
    let orders_config = AdapterConfig {
        connection: "sample_db".to_string(),
        description: Some("Order data from database".to_string()),
        source: AdapterSource::Database {
            table_name: "orders".to_string(),
        },
        columns: vec![],
    };
    config.upsert_adapter("orders", &orders_config)?.save()?;

    Ok(())
}

fn create_sample_models(config: &mut Config) -> Result<()> {
    // Staging: app_logs model
    let app_logs_config = ModelConfig {
        description: Some("Cleaned application logs".to_string()),
        sql: "SELECT
    timestamp::TIMESTAMP as event_time,
    user_id,
    action,
    device,
    duration,
    DATE(timestamp) as event_date
FROM app_logs
WHERE duration > 0"
            .to_string(),
    };
    config
        .upsert_model("staging_app_logs", &app_logs_config)?
        .save()?;

    // Marts: user_activity_summary model
    let user_activity_config = ModelConfig {
        description: Some("User activity summary".to_string()),
        sql: "SELECT
    u.user_id,
    u.name,
    u.email,
    COUNT(DISTINCT l.event_date) as active_days,
    COUNT(l.action) as total_actions,
    AVG(l.duration) as avg_duration,
    MAX(l.event_time) as last_activity
FROM users u
LEFT JOIN staging_app_logs l ON u.user_id = l.user_id
GROUP BY u.user_id, u.name, u.email"
            .to_string(),
    };
    config
        .upsert_model("user_activity_summary", &user_activity_config)?
        .save()?;

    // Marts: product_performance model
    let product_performance_config = ModelConfig {
        description: Some("Product performance metrics".to_string()),
        sql: "SELECT
    p.product_id,
    p.name as product_name,
    p.category,
    p.price,
    p.stock,
    COUNT(o.order_id) as order_count,
    SUM(o.quantity) as total_quantity_sold,
    SUM(o.total_amount) as total_revenue,
    AVG(o.total_amount) as avg_order_value
FROM products p
LEFT JOIN orders o ON p.product_id = o.product_id
GROUP BY p.product_id, p.name, p.category, p.price, p.stock
ORDER BY total_revenue DESC"
            .to_string(),
    };
    config
        .upsert_model("product_performance", &product_performance_config)?
        .save()?;

    Ok(())
}

pub fn create_sample_queries(config: &mut Config) -> Result<()> {
    // Top products query
    let top_products_config = QueryConfig {
        description: Some("Top 5 products by revenue".to_string()),
        sql: "SELECT
    product_name,
    category,
    total_revenue,
    order_count
FROM product_performance
ORDER BY total_revenue DESC
LIMIT 5"
            .to_string(),
    };
    config
        .upsert_query("top_products", &top_products_config)?
        .save()?;

    // Active users query
    let active_users_config = QueryConfig {
        description: Some("Most active users by action count".to_string()),
        sql: "SELECT
    name,
    email,
    total_actions,
    active_days,
    ROUND(avg_duration, 2) as avg_duration_seconds
FROM user_activity_summary
WHERE total_actions > 0
ORDER BY total_actions DESC
LIMIT 10"
            .to_string(),
    };
    config
        .upsert_query("active_users", &active_users_config)?
        .save()?;

    Ok(())
}

pub fn create_sample_dashboards(config: &mut Config) -> Result<()> {
    // First create the query files
    let revenue_query = QueryConfig {
        description: Some("Daily revenue trend query".to_string()),
        sql: "SELECT
    DATE(order_date) as date,
    SUM(total_amount) as daily_revenue
FROM orders
WHERE status = 'completed'
GROUP BY DATE(order_date)
ORDER BY date"
            .to_string(),
    };
    config
        .upsert_query("revenue_trend", &revenue_query)?
        .save()?;

    let category_query = QueryConfig {
        description: Some("Product sales by category query".to_string()),
        sql: "SELECT
    category,
    SUM(total_quantity_sold) as units_sold
FROM product_performance
GROUP BY category
ORDER BY units_sold DESC"
            .to_string(),
    };
    config
        .upsert_query("category_distribution", &category_query)?
        .save()?;

    let revenue_config = DashboardConfig {
        description: Some("Daily Revenue Trend".to_string()),
        query: "revenue_trend".to_string(),
        chart: ChartConfig {
            chart_type: ChartType::Line,
            x_column: "date".to_string(),
            y_column: "daily_revenue".to_string(),
        },
    };
    config
        .upsert_dashboard("revenue_trend", &revenue_config)?
        .save()?;

    let category_config = DashboardConfig {
        description: Some("Product Sales by Category".to_string()),
        query: "category_distribution".to_string(),
        chart: ChartConfig {
            chart_type: ChartType::Bar,
            x_column: "category".to_string(),
            y_column: "units_sold".to_string(),
        },
    };
    config
        .upsert_dashboard("category_distribution", &category_config)?
        .save()?;

    Ok(())
}

async fn create_sample_graph(config: &mut Config) -> Result<()> {
    let mut graph = Graph::load(&config.project_dir).await?;

    for name in config.adapters.keys() {
        graph.create_node(name, &[]);
    }

    for (model_name, model_config) in &config.models {
        let dependencies = dependent_tables(&model_config.sql).unwrap_or_default();

        let dependency_refs: Vec<&str> = dependencies.iter().map(|s| s.as_str()).collect();
        graph.create_node(model_name, &dependency_refs);
    }

    graph.save().await?;
    Ok(())
}
