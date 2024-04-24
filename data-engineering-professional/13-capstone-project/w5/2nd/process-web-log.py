from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
from datetime import timedelta


# Task 1 - Define the DAG arguments
default_args = {
    'owner': 'Hoon Lee',
    'start_date': days_ago(0),
    'email': ['hoon@somemail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


# Task 2 - Define the DAG
dag = DAG(
    'process_web_log',
    default_args=default_args,
    description='Process web logs',
    schedule_interval=timedelta(days=1),
)


# Task 3 - Create a task to extract data
extract_data = BashOperator(
    task_id='extract_data',
    bash_command='cut -d" " -f1 /home/project/airflow/dags/capstone/accesslog.txt > /home/project/airflow/dags/capstone/extracted_data.txt',
    dag=dag,
)

# Task 4 - Create a task to transform the data in the txt file
transform_data = BashOperator(
    task_id='transform_data',
    bash_command='grep 198.46.149.143 /home/project/airflow/dags/capstone/extracted_data.txt > /home/project/airflow/dags/capstone/transformed_data.txt',
    dag=dag,
)

# Task 5 - Create a task to load the data
load_data = BashOperator(
    task_id = 'load_data',
    bash_command='tar -cvf /home/project/airflow/dags/capstone/weblog.tar /home/project/airflow/dags/capstone/transformed_data.txt',
    dag=dag,
)

# Task 6 - Define the task pipeline
extract_data >> transform_data >> load_data
