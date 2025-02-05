from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

import boto3
import pandas as pd
from io import StringIO
from datetime import datetime, timedelta
from book_extraction import run_extract_data

bucket_name = 'books_pipeline_bucket'
current_year_month = datetime.now().strftime("%Y-%m")

def get_data_from_s3():
    s3 = boto3.client('s3')

    try:
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=current_year_month)

        if 'Contents' in response:
            for obj in response['Contents']:
                file_key = obj['Key']
                print(f"Found file: {file_key}")

                if file_key == current_year_month or file_key.startswith(current_year_month + '/'):
                    response = s3.get_object(Bucket=bucket_name, Key=file_key)
                    
                    df = pd.read_csv(StringIO(response['Body'].read().decode('utf-8')))
                    
                    # Print the DataFrame
                    print(f"DataFrame for file {file_key}:")
                    break  
        else:
            print(f"No files found with prefix: {current_year_month}")

    except Exception as e:
        print(f"Error: {e}")

    return df

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 2, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'schedule_interval': '@monthly'
}

dag = DAG(
    'books_pipeline',
    default_args=default_args,
    description='Extract books & Load book',
)

#tasks : 1) fetch multiple data (extract) 2) validate data to match column format 3) store data in datalake (s3)
extract_load_s3 = PythonOperator(
    task_id='get_load_s3',
    python_callable=run_extract_data,
    dag=dag,
)

create_table_task = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='books_connection',
    sql="""
    CREATE TABLE IF NOT EXISTS books (
        id SERIAL PRIMARY KEY,
        title TEXT NOT NULL,
        authors TEXT,
        price TEXT,
        rating TEXT
    );
    """,
    dag=dag,
)

insert_book_data_task = PythonOperator(
    task_id='insert_book_data',
    python_callable=insert_book_data_into_postgres,
    dag=dag,
)


fetch_book_data_task >> create_table_task >> insert_book_data_task