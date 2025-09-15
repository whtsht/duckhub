# DuckHub

A data infrastructure building tool powered by Duckdb

## Features
- Extract data from external data sources
- Store data using DuckDB and DuckLake
- Transform data using SQL
- Pipeline management based on dependency analysis
- Visualization through dashboards

## Getting Started

### Quick Start with Web UI
```bash
# Create a new project
duckhub new my-project

# Start the web interface
duckhub start my-project
```

## Core Concepts
- Connection: Connection and authentication information for external data sources
- Adapter: Extracts data from external data sources
- Model: Data transformation using SQL
- Graph: Represents dependencies between Adapters and Models
- Pipeline: Executes data processing based on the Graph
- Query: Executes SQL queries
- Dashboard: Data visualization
