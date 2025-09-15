use anyhow::Result;
use clap::{Parser, Subcommand};
use commands::{
    new::{create_gitignore, create_secret_key},
    samples::create_samples,
};
use core::config::{Config, project::ProjectConfig};

pub mod api;
pub mod commands;
pub mod core;
pub mod error_handle;
#[cfg(test)]
pub mod test_helpers;
pub mod ui;

use tracing_subscriber::EnvFilter;

fn setup_tracing() {
    #[cfg(debug_assertions)]
    {
        let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("debug"));
        tracing_subscriber::fmt().with_env_filter(filter).init();
    }

    #[cfg(not(debug_assertions))]
    {
        let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn"));
        tracing_subscriber::fmt().with_env_filter(filter).init();
    }
}

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    New {
        project_name: String,
    },
    Start {
        project_name: String,
        #[arg(short, long, default_value = "3015")]
        port: u16,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    let result = match &cli.command {
        Commands::New { project_name } => {
            let project_dir = std::env::current_dir()?.join(project_name);

            if project_dir.exists() {
                return Err(anyhow::anyhow!(
                    "Directory '{}' already exists",
                    project_dir.display()
                ));
            }

            std::fs::create_dir_all(&project_dir)?;

            let mut config = Config::new(project_dir.clone());
            config
                .add_project_setting(&ProjectConfig::default())?
                .save()?;

            create_gitignore(&project_dir)?;
            create_secret_key(&project_dir)?;
            create_samples(&mut config).await?;

            println!("✓ Project '{project_name}' created successfully");
            println!("  Run 'duckhub start {project_name}' to open the project");
            Ok(())
        }
        Commands::Start { project_name, port } => {
            let project_dir = std::env::current_dir()?.join(project_name);
            let config = Config::new(project_dir);

            setup_tracing();

            commands::start::execute_start(config, *port).await
        }
    };

    if let Err(err) = result {
        eprintln!("Error: {err}");
        std::process::exit(1);
    }

    Ok(())
}
