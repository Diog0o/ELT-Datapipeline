from datetime import datetime, timedelta
from airflow import DAG
from docker.types import Mount
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.docker.operators.docker import DockerOperator
import subprocess
import os

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

def run_elt_script():
    """Run the ELT script"""
    script_path = '/opt/airflow/elt_script/elt_script.py'
    
    # Check if script exists
    if not os.path.exists(script_path):
        raise FileNotFoundError(f"ELT script not found at {script_path}")
    
    # Run the script
    result = subprocess.run(
        ['python', script_path],
        capture_output=True, 
        text=True,
        cwd='/opt/airflow/elt_script'
    )
    
    if result.returncode != 0:
        print(f"STDOUT: {result.stdout}")
        print(f"STDERR: {result.stderr}")
        raise Exception(f"Script failed with error: {result.stderr}")
    else:
        print("ELT Script completed successfully")
        print(result.stdout)

# Create the DAG
dag = DAG(
    'elt_and_dbt',
    default_args=default_args,
    description='An ELT workflow with dbt',
    start_date=datetime(2025, 8, 15),
    catchup=False,
    schedule_interval=None,  # Manual trigger
    tags=['elt', 'dbt']
)

# Task 1: Run ELT Script
t1 = PythonOperator(
    task_id='run_elt_script',
    python_callable=run_elt_script,
    dag=dag
)

# Task 2: Run dbt transformations
t2 = DockerOperator(
    task_id='dbt_run',
    image='ghcr.io/dbt-labs/dbt-postgres:1.4.7',
    command=[
        "run",
        "--profiles-dir",
        "/root",
        "--project-dir",
        "/dbt",
        "--full-refresh"
    ],
    auto_remove=True,
    docker_url="unix://var/run/docker.sock",
    network_mode="elt_network",  # Use the same network as your containers
    mounts=[
        Mount(
            source='/opt/dbt',  # Use the path inside the container
            target='/dbt', 
            type='bind'
        ),
        Mount(
            source='/root/.dbt', 
            target='/root', 
            type='bind'
        ),
    ],
    dag=dag
)

# Set task dependencies
t1 >> t2