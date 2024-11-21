from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import snowflake.connector
import boto3
import os


def execute_snowflake_query(**kwargs):
    conn = snowflake.connector.connect(
        user=os.getenv('SVC_SF_ENTITY_RESOLUTION_NONPROD'),
        password=os.getenv('4O9uFe*a9?!aGltHLB+S'),
        account=os.getenv('DATAHUB'),
        warehouse='ER_WH_NONPROD',
        database='DEV_ENTITY_RESOLUTION',
        schema='PF',
    )
    query = kwargs.get('query', 'SELECT 1')
    cursor = conn.cursor()
    cursor.execute(query)
    result = cursor.fetchall()
    cursor.close()
    conn.close()
    return result

def interact_with_s3(bucket_name, file_key, local_file_path, operation='upload'):
    s3 = boto3.client('s3')
    if operation == 'upload':
        s3.upload_file(local_file_path, bucket_name, file_key)
    elif operation == 'download':
        s3.download_file(bucket_name, file_key, local_file_path)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

with DAG(
    'snowflake_s3_integration',
    default_args=default_args,
    description='Integrate Snowflake with S3 using MWAA',
    schedule_interval='@daily',
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    # Example task to execute a Snowflake query
    query_snowflake = PythonOperator(
        task_id='query_snowflake',
        python_callable=execute_snowflake_query,
        op_kwargs={'query': 'SELECT * FROM MEMBER_DIM LIMIT 10'},
    )

    # Example task to upload a file to S3
    upload_to_s3 = PythonOperator(
        task_id='upload_to_s3',
        python_callable=interact_with_s3,
        op_kwargs={
            'bucket_name': 'pf-aas-airflow-nonprod-config',
            'file_key': 'tarun_sample/data.csv',
            'local_file_path': '/tmp/data.csv',
            'operation': 'upload',
        },
    )

    query_snowflake >> upload_to_s3
