# Task 2: Create the imports block.
# import the libraries
from datetime import timedelta
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
# Operators; we need this to write tasks!
from airflow.operators.bash_operator import BashOperator
# This makes scheduling easy
from airflow.utils.dates import days_ago

# Task 3: Create the DAG Arguments block. You can use the default settings.
#defining DAG arguments
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'Ramesh Sannareddy',
    'start_date': days_ago(0),
    'email': ['ramesh@somemail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Task 4: Create the DAG definition block. The DAG should run daily.
# defining the DAG
# define the DAG
dag = DAG(
    'ETL_Server_Access_Log_Processing',
    default_args=default_args,
    description='My first DAG',
    schedule_interval=timedelta(days=1),
)

# Task 5: Create the task extract_transform_and_load to call the shell script.
# define the tasks
#define the task named extract_transform_and_load to call the shell script
#calling the shell script
extract_transform_and_load = BashOperator(
    task_id="extract_transform_and_load",
    bash_command="/home/project/airflow/dags/ETL_Server_Access_Log_Processing.sh ",
    dag=dag,
)

# Task 6: Create the task pipeline block.
# task pipeline
extract_transform_and_load

# Task 7: Submit the DAG.
# cp  ETL_Server_Access_Log_Processing.py $AIRFLOW_HOME/dags

# Task 8: Submit the shell script to dags folder.
# cp  ETL_Server_Access_Log_Processing.sh $AIRFLOW_HOME/dags

# Task 9: Change the permission to read shell script.
# cd airflow/dags
# chmod 777 ETL_Server_Access_Log_Processing.sh

# Task 10: Verify if the DAG is submitted.
# airflow dags list