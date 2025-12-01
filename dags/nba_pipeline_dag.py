# Fix for Python 3.12 compatibility with snowflake-sqlalchemy
import collections.abc
if not hasattr(collections, 'Sequence'):
    collections.Sequence = collections.abc.Sequence

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime, timedelta
import pandas as pd
from snowflake.connector.pandas_tools import write_pandas

default_args = {
    'owner': 'NBA Pipeline',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

def extract_kaggle_data():
    """
    Extract NBA player statistics data from Kaggle dataset.
    Downloads Seasons_Stats.csv and saves to temporary file.
    """
    import kagglehub
    from kagglehub import KaggleDatasetAdapter
    
    print("📥 EXTRACT: Fetching NBA dataset from Kaggle...")
    df = kagglehub.load_dataset(
        KaggleDatasetAdapter.PANDAS,
        "drgilermo/nba-players-stats",
        "Seasons_Stats.csv"
    )
    
    print(f"✅ Extracted {len(df)} records with {len(df.columns)} columns")
    
    # Save to temporary file
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"/tmp/nba_stats_{timestamp}.csv"
    df.to_csv(filename, index=False)
    
    print(f"✅ Saved extracted data to {filename}")
    return filename

def load_to_snowflake(**context):
    """
    Load extracted NBA data into Snowflake staging table.
    """
    # Get the CSV file path from previous task
    ti = context['ti']
    csv_file = ti.xcom_pull(task_ids='extract_kaggle_data')
    
    print(f"📤 LOAD: Loading data from {csv_file} to Snowflake staging...")
    
    # Read the CSV file
    df = pd.read_csv(csv_file)
    
    # Get Snowflake connection from Airflow UI (uses conn_id directly)
    snowflake_hook = SnowflakeHook(snowflake_conn_id="snowflake_default")
    conn = snowflake_hook.get_conn()
    cursor = conn.cursor()
    
    # Set context
    cursor.execute("USE ROLE TRAINING_ROLE;")
    cursor.execute("USE WAREHOUSE TRAINING_WH;")
    cursor.execute("USE DATABASE MLDS430;")
    cursor.execute("CREATE SCHEMA IF NOT EXISTS MARMOT_FINAL;")
    cursor.execute("USE SCHEMA MARMOT_FINAL;")
    
    # Set database for write_pandas
    database = 'MLDS430'
    
    # Clean column names for Snowflake compatibility
    df.columns = df.columns.str.replace(' ', '_').str.replace('%', '_PCT').str.replace('/', '_')
    df.columns = df.columns.str.replace('3P', 'ThreeP').str.replace('2P', 'TwoP')
    df.columns = [col.replace('Unnamed: 0', 'Unnamed_0') for col in df.columns]
    
    # Add extraction timestamp
    df['extraction_timestamp'] = datetime.now()
    
    # Load to Snowflake
    success, nchunks, nrows, _ = write_pandas(
        conn,
        df,
        'RAW_NBA_STATS',
        database=database,
        schema='MARMOT_FINAL',
        auto_create_table=True,
        overwrite=True
    )
    
    cursor.close()
    
    if success:
        print(f"✅ Successfully loaded {nrows} records to MARMOT_FINAL.RAW_NBA_STATS")
    else:
        raise Exception("Failed to load data to Snowflake")

with DAG(
    'nba_pipeline_dag',
    description='NBA Pipeline: Kaggle Extract → Snowflake Staging → dbt Transform → Streamlit Visualization',
    schedule='@daily',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=['nba', 'etl', 'snowflake', 'kaggle', 'dbt', 'streamlit'],
) as dag:

    # Step 1: EXTRACT - Kaggle API
    extract = PythonOperator(
        task_id='extract_kaggle_data',
        python_callable=extract_kaggle_data
    )
    
    # Step 2: LOAD - Snowflake staging
    load = PythonOperator(
        task_id='load_to_snowflake',
        python_callable=load_to_snowflake
    )
    
    # Step 3: TRANSFORM - dbt models
    transform = BashOperator(
        task_id='transform_with_dbt',
        bash_command="""
            cd /usr/local/airflow/include/nba_dw_project && \
            dbt run --profiles-dir .
        """,
    )
    
    # Step 4: VERIFY - Snowflake query (raw data)
    verify_raw = SQLExecuteQueryOperator(
        task_id='verify_raw_data',
        conn_id='snowflake_default',
        sql="""
            USE ROLE TRAINING_ROLE;
            USE WAREHOUSE TRAINING_WH;
            USE DATABASE MLDS430;
            USE SCHEMA MARMOT_FINAL;
            SELECT 
                COUNT(*) as total_records,
                MIN("Year") as earliest_year,
                MAX("Year") as latest_year,
                COUNT(DISTINCT "Player") as unique_players
            FROM RAW_NBA_STATS;
        """,
        do_xcom_push=True,
    )
    
    # Step 5: VERIFY - dbt transformed data
    verify_transformed = SQLExecuteQueryOperator(
        task_id='verify_transformed_data',
        conn_id='snowflake_default',
        sql="""
            USE ROLE TRAINING_ROLE;
            USE WAREHOUSE TRAINING_WH;
            USE DATABASE MLDS430;
            USE SCHEMA MARMOT_FINAL;
            SELECT 
                'stg_nba_stats' as model_name,
                COUNT(*) as record_count
            FROM stg_nba_stats
            UNION ALL
            SELECT 
                'fact_player_season' as model_name,
                COUNT(*) as record_count
            FROM fact_player_season
            UNION ALL
            SELECT 
                'fact_player_season_advanced' as model_name,
                COUNT(*) as record_count
            FROM fact_player_season_advanced;
        """,
        do_xcom_push=True,
    )

    # Print verification results
    def print_raw_results(**context):
        results = context['ti'].xcom_pull(task_ids='verify_raw_data')
        print("=" * 50)
        print("📊 RAW DATA VERIFICATION:")
        print(f"Total Records: {results[0][0]}")
        print(f"Earliest Year: {results[0][1]}")
        print(f"Latest Year: {results[0][2]}")
        print(f"Unique Players: {results[0][3]}")
        print("=" * 50)

    def print_transformed_results(**context):
        results = context['ti'].xcom_pull(task_ids='verify_transformed_data')
        print("=" * 50)
        print("📊 TRANSFORMED DATA VERIFICATION:")
        for row in results:
            print(f"{row[0]}: {row[1]} records")
        print("=" * 50)

    print_raw_results_task = PythonOperator(
        task_id='print_raw_verification_results',
        python_callable=print_raw_results,
    )

    print_transformed_results_task = PythonOperator(
        task_id='print_transformed_verification_results',
        python_callable=print_transformed_results,
    )
    
    # Step 6: VISUALIZE - Verify visualization data is ready
    def verify_visualization_data(**context):
        """
        Verify that visualization data is ready by querying transformed tables.
        This ensures the Streamlit app has data to display.
        """
        snowflake_hook = SnowflakeHook(snowflake_conn_id="snowflake_default")
        conn = snowflake_hook.get_conn()
        cursor = conn.cursor()
        
        cursor.execute("USE ROLE TRAINING_ROLE;")
        cursor.execute("USE WAREHOUSE TRAINING_WH;")
        cursor.execute("USE DATABASE MLDS430;")
        cursor.execute("USE SCHEMA MARMOT_FINAL;")
        
        # Verify data exists for visualization
        cursor.execute("SELECT COUNT(*) FROM fact_player_season;")
        fact_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM fact_player_season_advanced;")
        advanced_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(DISTINCT player_name) FROM fact_player_season;")
        unique_players = cursor.fetchone()[0]
        
        cursor.close()
        
        if fact_count > 0 and advanced_count > 0:
            print("=" * 50)
            print("📊 VISUALIZATION DATA VERIFICATION:")
            print(f"✅ Fact Player Season records: {fact_count}")
            print(f"✅ Fact Player Season Advanced records: {advanced_count}")
            print(f"✅ Unique players: {unique_players}")
            print("=" * 50)
            print("✅ Streamlit dashboard data is ready!")
            return True
        else:
            raise Exception("Visualization data not ready - tables are empty")
    
    verify_viz = PythonOperator(
        task_id='verify_visualization_data',
        python_callable=verify_visualization_data,
    )
    
    # Pipeline: Extract → Load → Transform → Verify → Visualize
    extract >> load >> transform >> verify_raw >> print_raw_results_task >> verify_transformed >> print_transformed_results_task >> verify_viz
