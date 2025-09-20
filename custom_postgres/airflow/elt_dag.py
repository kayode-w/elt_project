import sys
import logging
import subprocess
from pathlib import Path
from datetime import timedelta, datetime

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.models.baseoperator import chain
from airflow.utils.dates import days_ago
from docker.types import Mount


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False
    # 'retry_delay': timedelta(minutes=5),
    # 'start_date': datetime(2023, 1, 1),
}

def run_elt_script():
    elt_path: Path = Path("/opt/airflow/dags/elt/elt_script.py")
    if not elt_path.is_file():
        raise AirflowException(f"ELT script not found: {elt_path}")

    try:
        res = subprocess.run(
            [sys.executable, str(elt_path)],
            check=True,
            capture_output=True,
            text=True,
        )
        logging.info("ELT script output:\n%s", res.stdout)
        if res.stderr:
            logging.warning("ELT script stderr:\n%s", res.stderr)
    except subprocess.CalledProcessError as e:
        logging.error("ELT script failed (exit %s):\n%s", e.returncode, e.stderr)
        raise AirflowException(f"ELT script failed (exit {e.returncode})") from e


dag = DAG(
    'elt_and_dbt',
    default_args = default_args,
    description = 'A DAG to run ELT from source to destination Postgres using Docker',
    start_date = datetime(2025, 9, 21),
    catchup = False
)
#Dag tasks
# task_1 = PythonOperator(
#     task_id='run_elt_script',
#     python_callable=run_elt_script,
#     dag = dag
#     )

# task_2 = DockerOperator(
#     task_id="dbt_run",
#     image = "my-dbt:1.7-pg",
#     command = ["run", "--profiles-dir", "/root","--project-dir", "/dbt"],
#     auto_remove = True, #auto remove the container when it finishes running dbt commands
#     docker_url = "unix://var/run/docker.sock",
#     network_mode = "bridge", 
#     mounts = [
#         Mount(source="/Users/kayodew/Desktop/python_files/Envirounments/elt_practice/custom_postgres", 
#               target="/dbt", type="bind"),
#         Mount(source="/Users/kayodew/.dbt", 
#               target="/root", type="bind")
#              ],
#     dag = dag
# )

# task_1 >> task_2

task_1 = PythonOperator(
    task_id='run_elt_script',
    python_callable=run_elt_script,
    dag=dag
)

task_2 = DockerOperator(
    task_id="dbt_run",
    image="my-dbt:1.7-pg",
    command=["run", "--profiles-dir", "/root/.dbt", "--project-dir", "/dbt"],
    auto_remove = 'force',
    docker_url="unix://var/run/docker.sock",
    network_mode="elt_network",  # <- use your actual compose network name
    mounts=[
        Mount(
            source="/Users/kayodew/Desktop/python_files/Envirounments/elt_practice/custom_postgres",
            target="/dbt", type="bind"
        ),
        Mount(
            source="/Users/kayodew/.dbt",
            target="/root/.dbt", type="bind"
        ),
    ],
    dag=dag,
)

task_1 >> task_2

