from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import requests
import boto3
from io import StringIO
import os

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def fetch_data_and_upload_to_s3():
    # Function to fetch data and upload to S3
    api_key = os.getenv('API_KEY')
    aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
    aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')

    # Request headers
    headers = {
        "api-key": api_key,
    }

    params = {"resource_id": "768d40be-4848-4cdb-b4f6-e1ce42b682eb", "limit": 500}
    r = requests.get(
        "https://opend.data.go.th/get-ckan/datastore_search", params, headers=headers
    )
    if r.ok:
        j = r.json()
        records = j["result"]["records"]
        df = pd.DataFrame(records)

        # Convert DataFrame to CSV string in memory
        csv_buffer = StringIO()
        
        df.to_csv(csv_buffer, index=False)
        csv_buffer.seek(0)

        # Create an S3 client
        session = boto3.Session(
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
        )
        s3 = session.client("s3")
        
        S3_BUCKET_NAME = "opendatagoth-dw-project"
        S3_FOLDER_ROUTE = "python-scrape/"

        # Upload CSV string directly to S3
        s3_filename = S3_FOLDER_ROUTE + "income_data.csv"
        s3.put_object(Bucket=S3_BUCKET_NAME, Key=s3_filename, Body=csv_buffer.getvalue())

        print("Data saved to S3 bucket:", s3_filename)
    else:
        print("Failed to retrieve data from the API")

def define_dag():
    # Function to define the DAG
    with DAG(
        'daily_data_update',
        default_args=default_args,
        description='A DAG to update data daily',
        schedule_interval=timedelta(days=1),
    ) as dag:

        fetch_data_task = PythonOperator(
            task_id='fetch_data_and_upload_to_s3',
            python_callable=fetch_data_and_upload_to_s3,
        )

    return dag

dag = define_dag()