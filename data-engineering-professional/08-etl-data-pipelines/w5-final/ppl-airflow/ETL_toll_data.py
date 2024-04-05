from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
from datetime import datetime as dt
from datetime import timedelta

# Task 1.1 - Define DAG arguments
default_args = {
    'owner': 'Hoon Lee',
    'start_date': days_ago(0),
    'email': ['hoonlee@somemail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Task 1.2 - Define the DAG
dag = DAG(
    dag_id='ETL_toll_data',
    schedule_interval=timedelta(days=1),
    default_args=default_args,
    description='Apache Airflow Final Assignment'
)

# Task 1.3 - Create a task to unzip data
unzip_data = BashOperator(
    task_id='unzip_data',
    bash_command="tar -xvzf /home/project/airflow/dags/finalassignment/tolldata.tgz -C /home/project/airflow/dags/finalassignment/staging",
    dag=dag
)

# Task 1.4 - Create a task to extract data from csv file
# This task should extract the fields 
# Rowid, Timestamp, Anonymized Vehicle number, and Vehicle type 
# from the vehicle-data.csv file 
# and save them into a file named csv_data.csv.
extract_data_from_csv = BashOperator(
    task_id='extract_data_from_csv',
    bash_command="cut -d ',' -f1-4  /home/project/airflow/dags/finalassignment/staging/vehicle-data.csv > /home/project/airflow/dags/finalassignment/staging/csv_data.csv",
    dag=dag
)

# Task 1.5 - Create a task to extract data from tsv file
# This task should extract the fields 
# Number of axles, Tollplaza id, and Tollplaza code 
# from the tollplaza-data.tsv file 
# and save it into a file named tsv_data.csv.
extract_data_from_tsv = BashOperator(
    task_id='extract_data_from_tsv',
    bash_command="cut -f5-7 /home/project/airflow/dags/finalassignment/staging/tollplaza-data.tsv | tr '\\t' ',' | sed 's/\\r$//' > /home/project/airflow/dags/finalassignment/staging/tsv_data.csv",
    dag=dag
)

# Task 1.6 - Create a task to extract data from fixed width file
# This task should extract the fields 
# Type of Payment code, and Vehicle Code 
# from the fixed width file payment-data.txt  | tr -d '\\n'
# and save it into a file named fixed_width_data.csv.
extract_data_from_fixed_width = BashOperator(
    task_id='extract_data_from_fixed_width',
    bash_command="cut -b 59-68 /home/project/airflow/dags/finalassignment/staging/payment-data.txt | tr ' ' ',' > /home/project/airflow/dags/finalassignment/staging/fixed_width_data.csv",
    dag=dag
)

# Task 1.7 - Create a task to consolidate data extracted from previous tasks
consolidate_data = BashOperator(
    task_id='consolidate_data',
    bash_command="paste -d ',' /home/project/airflow/dags/finalassignment/staging/csv_data.csv /home/project/airflow/dags/finalassignment/staging/tsv_data.csv /home/project/airflow/dags/finalassignment/staging/fixed_width_data.csv > /home/project/airflow/dags/finalassignment/staging/extracted_data.csv",
    dag=dag
)

# Task 1.8 - Transform and load the data
# transform the vehicle_type field in extracted_data.csv into capital letters 
# and save it into a file named transformed_data.csv in the staging directory.
transform_data = BashOperator(
    task_id='transform_data',
    bash_command="awk 'BEGIN {FS=OFS=\",\"} { $4 = toupper($4) } 1' /home/project/airflow/dags/finalassignment/staging/extracted_data.csv > /home/project/airflow/dags/finalassignment/staging/transformed_data.csv",
    dag=dag
)

# transform_data = BashOperator(
#     task_id='transform_data',
#     bash_command='tr "[a-z]" "[A-Z]" < /home/project/airflow/dags/finalassignment/staging/extracted_data.csv > /home/project/airflow/dags/finalassignment/staging/transformed_data.csv',
#     dag=dag
# )

# Task 1.9 - Define the task pipeline
unzip_data \
    >> extract_data_from_csv \
    >> extract_data_from_tsv \
    >> extract_data_from_fixed_width \
    >> consolidate_data \
    >> transform_data

