from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime, timedelta
import yfinance as yf
import pandas as pd
import os

DBT_PROJECT_DIR = "/opt/airflow/dbt" 


def return_snowflake_conn():
    """Connect to Snowflake using Airflow connection ID."""
    hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
    conn = hook.get_conn()
    return conn.cursor()

@task(execution_timeout=timedelta(minutes=30))
def extract_and_load():
    """
    Extract stock data from Yahoo Finance (AAPL & GOOG)
    and perform FULL REFRESH (delete + insert) into Snowflake.
    """
    stocks = ["AAPL", "GOOG"]
    frames = []

    # Extract data
    for stock in stocks:
        try:
            ticker = yf.Ticker(stock)
            df = ticker.history(period="180d", timeout=10)

            if df.empty:
                print(f"Warning: No data returned for {stock}")
                continue

            df.reset_index(inplace=True)
            df.rename(
                columns={
                    "Date": "date",
                    "Open": "open",
                    "High": "high",
                    "Low": "low",
                    "Close": "close",
                    "Volume": "volume",
                },
                inplace=True,
            )

            df["date"] = pd.to_datetime(df["date"]).dt.date
            df["volume"] = df["volume"].fillna(0).astype(int)
            for col in ["open", "high", "low", "close"]:
                df[col] = df[col].astype(float)

            df["symbol"] = stock
            frames.append(df[["symbol", "date", "open", "high", "low", "close", "volume"]])
            print(f"Extracted {len(df)} records for {stock}")

        except Exception as e:
            print(f"Error extracting data for {stock}: {str(e)}")

    if not frames:
        raise ValueError("No stock data extracted")

    df_final = pd.concat(frames, ignore_index=True)
    print(f"Total records to load: {len(df_final)}")

    rows = list(df_final.itertuples(index=False, name=None))
    target_table = "RAW.STOCK_DATA_LAB"

    cur = return_snowflake_conn()

    try:
        cur.execute("BEGIN;")
        cur.execute("CREATE DATABASE IF NOT EXISTS USER_DB_JACKAL")
        cur.execute("USE DATABASE USER_DB_JACKAL")
        cur.execute("CREATE SCHEMA IF NOT EXISTS RAW")

        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {target_table} (
                symbol VARCHAR(10) NOT NULL,
                date DATE NOT NULL,
                open FLOAT,
                high FLOAT,
                low FLOAT,
                close FLOAT,
                volume NUMBER(38,0),
                PRIMARY KEY (symbol, date)
            )
            """
        )
        print(f"Target table {target_table} ready")

        # FULL REFRESH
        cur.execute(f"DELETE FROM {target_table}")
        print(f"Existing data deleted from {target_table}")

        insert_sql = f"""
            INSERT INTO {target_table} (symbol, date, open, high, low, close, volume)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        cur.executemany(insert_sql, rows)
        print(f"Inserted {len(rows)} records into {target_table}")

        cur.execute("COMMIT;")
        print("Transaction committed successfully")

    except Exception as e:
        print(f"Error during load: {str(e)}")
        try:
            cur.execute("ROLLBACK;")
            print("Transaction rolled back")
        except Exception as rollback_error:
            print(f"Rollback error: {str(rollback_error)}")
        raise
    finally:
        try:
            cur.close()
        except Exception:
            pass

# DAG definition
with DAG(
    dag_id="StockData_ELT_DBT",
    start_date=datetime(2025, 11, 1),
    description="Full refresh ELT pipeline for AAPL & GOOG stock data to Snowflake, with dbt transformations",
    schedule="0 3 * * *",  # Daily 3 AM
    catchup=False,
    max_active_runs=1,
    concurrency=1,
    dagrun_timeout=timedelta(hours=3),
    tags=["ELT", "dbt", "snowflake", "stocks"],
) as dag:

    load_task = extract_and_load()

    print_env = BashOperator(
        task_id="debug_dbt_env",
        bash_command=f'echo "DBT directory: {DBT_PROJECT_DIR}"',
    )
    dbt_deps = BashOperator(
    task_id="dbt_install_dependencies",
    bash_command=f"dbt deps --profiles-dir {DBT_PROJECT_DIR} --project-dir {DBT_PROJECT_DIR}",
)

    dbt_run = BashOperator(
        task_id="dbt_run_transformations",
        bash_command=f"dbt run --profiles-dir {DBT_PROJECT_DIR} --project-dir {DBT_PROJECT_DIR}",
    )

    dbt_test = BashOperator(
        task_id="dbt_test_data_quality",
        bash_command=f"dbt test --profiles-dir {DBT_PROJECT_DIR} --project-dir {DBT_PROJECT_DIR}",
    )

    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot_history",
        bash_command=f"dbt snapshot --profiles-dir {DBT_PROJECT_DIR} --project-dir {DBT_PROJECT_DIR}",
    )

    dbt_docs_generate = BashOperator(
        task_id="dbt_docs_generate",
        bash_command=f"dbt docs generate --profiles-dir {DBT_PROJECT_DIR} --project-dir {DBT_PROJECT_DIR}",
    )

load_task >> print_env >> dbt_deps >> dbt_run >> dbt_test >> dbt_snapshot >> dbt_docs_generate
