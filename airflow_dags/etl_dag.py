# airflow_dags/etl_dag.py

from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

# You may need to add your project directory to sys.path if not installed as a package
import sys
import os
PROJECT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if PROJECT_DIR not in sys.path:
    sys.path.insert(0, PROJECT_DIR)

def run_etl():
    """
    This function imports and runs the ETL process.
    It calls your extract, transform, and load functions.
    """
    from scripts.extract_data import extract_all_data
    from scripts.transform_data import transform_all
    from scripts.load_to_postgres import load_to_postgres

    # Extract
    customers, sales, products, payments, marketing_ads, customer_support = extract_all_data()

    # Transform
    sales_mart, marketing_mart, support_mart = transform_all(
        customers, sales, products, payments, marketing_ads, customer_support
    )

    # Load to PostgreSQL (ensure your db_url is set properly in load_to_postgres.py)
    load_to_postgres(sales_mart, "sales_mart")
    load_to_postgres(marketing_mart, "marketing_mart")
    load_to_postgres(support_mart, "support_mart")

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 3, 24),
    'email': ['your_email@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'etl_dag',
    default_args=default_args,
    description='ETL DAG for Retail Data Engineering Project',
    schedule_interval=timedelta(days=1),
)

# Define the Python task
etl_task = PythonOperator(
    task_id='run_etl',
    python_callable=run_etl,
    dag=dag,
)

etl_task
