use crate::core::config::Config;
use anyhow::Result;
use tokio::task::JoinHandle;

pub async fn execute_start(mut config: Config, port: u16) -> Result<()> {
    if !config.project_dir.join("project.yml").exists() {
        return Err(anyhow::anyhow!(
            "The directory is not a valid DuckHub project (missing project.yml)",
        ));
    }

    config.load()?;

    let api_handle: JoinHandle<Result<()>> =
        tokio::spawn(async move { crate::api::main(config).await });

    let ui_handle: JoinHandle<Result<()>> =
        tokio::spawn(async move { crate::ui::start_ui_server().await });

    if std::env::var("MANUAL_OPEN_BROWSER").is_err() {
        if open::that("http://localhost:8015").is_ok() {
            println!("✓ Browser opened successfully");
        } else {
            println!("⚠ Could not open browser automatically");
        }
    }

    println!("\n🚀 DuckHub is running!");
    println!("   API: http://localhost:{port}");
    println!("   UI: http://localhost:8015");
    println!("\nPress Ctrl+C to stop");

    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            println!("\nShutting down...");
        }
        api_result = api_handle => {
            match api_result {
                Ok(Ok(())) => println!("API server stopped"),
                Ok(Err(e)) => eprintln!("API server error: {e}"),
                Err(e) => eprintln!("API server task error: {e}"),
            }
        }
        ui_result = ui_handle => {
            match ui_result {
                Ok(Ok(())) => println!("UI server stopped"),
                Ok(Err(e)) => eprintln!("UI server error: {e}"),
                Err(e) => eprintln!("UI server task error: {e}"),
            }
        }
    }

    Ok(())
}
