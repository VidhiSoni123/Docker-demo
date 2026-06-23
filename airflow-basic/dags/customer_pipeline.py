from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
from datetime import datetime
import os

INPUT_FILE = "/opt/airflow/data/customers.csv"
VALIDATED_FILE = "/opt/airflow/data/validated_customers.csv"

def extract_customer():
    print(f"Reading data from {INPUT_FILE}")
    if not os.path.exists(INPUT_FILE):
        raise FileNotFoundError(f"Source file not found at {INPUT_FILE}")
    df = pd.read_csv(INPUT_FILE)
    print(f"Extracted {len(df)} total customers records")
    return INPUT_FILE  # Saved automatically to XCom
    
def validate_customer(ti):
    print("Validating customer profiles...")
    input_path = ti.xcom_pull(task_ids='run_extract_customer')
    
    df = pd.read_csv(input_path)
    # Updated to match your exact CSV headers: customer_id, name, email
    validated_df = df.dropna(subset=['customer_id', 'name', 'email'])
    validated_df.to_csv(VALIDATED_FILE, index=False)
    print(f"Validation successful. Cleaned records saved to {VALIDATED_FILE}")
    return VALIDATED_FILE

def load_database(ti):
    validated_path = ti.xcom_pull(task_ids='run_validate_customer')
    print(f"Loading data from {validated_path} into target system...")
    
    if os.path.exists(validated_path):
        df = pd.read_csv(validated_path)
        print(f"Successfully simulated loading {len(df)} rows into database.")
    else:
        print("No validated records available to load.")

def customer_email(ti):
    validated_path = ti.xcom_pull(task_ids='run_validate_customer')
    df = pd.read_csv(validated_path)
    for index, row in df.iterrows():
        # Updated to use row['email'] instead of row['mail']
        print(f"Sending welcome email to {row['name']} ({row['email']})")
    

with DAG(
    dag_id="customer_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False
) as dag:
    
    run_extract_customer = PythonOperator(
        task_id="run_extract_customer",
        python_callable=extract_customer
    )
    run_validate_customer = PythonOperator(
        task_id="run_validate_customer",
        python_callable=validate_customer
    )
    run_load_database = PythonOperator(
        task_id="run_load_database",
        python_callable=load_database
    )
    run_customer_email = PythonOperator(
        task_id="run_customer_email",
        python_callable=customer_email
    )

    run_extract_customer >> run_validate_customer >> run_load_database >> run_customer_email