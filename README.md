# NBA Data Pipeline - End-to-End ETL Project

A complete data pipeline that extracts NBA player statistics from Kaggle, loads them into Snowflake, transforms them using dbt, and visualizes them in an interactive Streamlit dashboard.

## 🏀 Project Overview

This project implements a modern data engineering pipeline following best practices:

- **Extract**: Download NBA player statistics from Kaggle
- **Load**: Store raw data in Snowflake data warehouse
- **Transform**: Clean and transform data using dbt models
- **Visualize**: Interactive dashboard built with Streamlit

## 📊 Pipeline Architecture

```
Kaggle Dataset → Airflow Extract → Snowflake (RAW_NBA_STATS) 
    → dbt Transform → Fact Tables → Streamlit Dashboard
```

### Data Flow

1. **Extract** (`extract_kaggle_data`): Downloads `Seasons_Stats.csv` from Kaggle dataset
2. **Load** (`load_to_snowflake`): Loads raw data into `MARMOT_FINAL.RAW_NBA_STATS` table
3. **Transform** (`transform_with_dbt`): Runs dbt models to create:
   - `stg_nba_stats` (staging view)
   - `fact_player_season` (fact table)
   - `fact_player_season_advanced` (advanced metrics fact table)
4. **Verify**: Validates data at each stage
5. **Visualize**: Streamlit dashboard queries transformed data directly from Snowflake

## 🚀 Quick Start

### Prerequisites

- Docker Desktop installed and running
- Astronomer CLI installed (`pip install astronomer-cosmos`)
- Snowflake account with credentials configured
- Kaggle API credentials (optional, for direct API access)

### Setup

1. **Clone the repository** (if applicable)

2. **Configure Snowflake Connection**
   - Open Airflow UI at `http://localhost:8080`
   - Go to Admin → Connections
   - Add/edit connection `snowflake_default`:
     - Connection Type: `Snowflake`
     - Host: `<your-account>.snowflakecomputing.com`
     - Schema: `MARMOT_FINAL`
     - Database: `MLDS430`
     - Login: Your Snowflake username
     - Password: Your Snowflake password
     - Extra (JSON):
       ```json
       {
         "account": "<your-account>",
         "warehouse": "TRAINING_WH",
         "database": "MLDS430",
         "role": "TRAINING_ROLE"
       }
       ```

3. **Place Private Key** (if using key authentication)
   - Copy your `rsa_key.pem` file to `include/rsa_key.pem`

4. **Start Airflow**
   ```bash
   astro dev start
   ```
   This will:
   - Build Docker containers
   - Install dependencies from `requirements.txt`
   - Start Airflow UI at `http://localhost:8080`

### Running the Pipeline

1. **Access Airflow UI**
   - Open `http://localhost:8080`
   - Default credentials: `admin` / `admin`

2. **Trigger the DAG**
   - Find `nba_pipeline_dag` in the DAG list
   - Toggle it ON (if paused)
   - Click "Play" button to trigger manually
   - Monitor progress in the Graph View

### Running Streamlit Dashboard

1. **Install Streamlit** (if running locally)
   ```bash
   pip install streamlit plotly snowflake-connector-python pandas cryptography
   ```

2. **Run the Dashboard**
   ```bash
   streamlit run include/streamlit_app.py
   ```

3. **Access Dashboard**
   - Opens automatically at `http://localhost:8501`
   - Or navigate manually to the URL

4. **Dashboard Features**
   - **Overview Tab**: Season trends, top players, key metrics
   - **Player Performance Tab**: Individual player career analysis
   - **Top Performers Tab**: Top 20 players by VORP
   - **Interactive Filters**: Season and player selection

## 📁 Project Structure

```
dw_final_project/
├── dags/
│   └── nba_pipeline_dag.py          # Main Airflow DAG
├── include/
│   ├── streamlit_app.py              # Streamlit dashboard
│   └── nba_dw_project/               # dbt project
│       ├── dbt_project.yml           # dbt configuration
│       ├── profiles.yml              # Snowflake connection config
│       └── models/
│           ├── staging/
│           │   ├── _sources.yml      # Source definitions
│           │   └── stg_nba_stats.sql # Staging model
│           └── marts/
│               ├── fact_player_season.sql
│               └── fact_player_season_advanced.sql
├── requirements.txt                  # Python dependencies
├── airflow_settings.yaml             # Airflow local config
└── README.md                         # This file
```

## 🔧 Key Components

### Airflow DAG (`nba_pipeline_dag.py`)

**Tasks:**
- `extract_kaggle_data`: Downloads data from Kaggle
- `load_to_snowflake`: Loads data to Snowflake staging table
- `transform_with_dbt`: Runs dbt transformations
- `verify_raw_data`: Validates raw data
- `verify_transformed_data`: Validates transformed data
- `verify_visualization_data`: Ensures data is ready for dashboard

**Schedule**: Daily (`@daily`)

### dbt Models

**Staging Layer** (`stg_nba_stats`):
- Filters data from 1984 onwards
- Cleans column names
- Consolidates multi-team players
- Materialized as a view

**Marts Layer**:
- `fact_player_season`: Core player-season statistics with per-game metrics
- `fact_player_season_advanced`: Advanced metrics (PER, VORP, Win Shares, etc.)
- Materialized as tables

### Streamlit Dashboard

**Features:**
- Real-time queries from Snowflake
- 4+ interactive visualizations
- Season and player filters
- Multiple analysis views

## 📦 Dependencies

See `requirements.txt` for complete list:

- `pandas`: Data manipulation
- `kagglehub`: Kaggle dataset access
- `snowflake-connector-python`: Snowflake connectivity
- `dbt-core` & `dbt-snowflake`: Data transformations
- `streamlit` & `plotly`: Dashboard and visualizations