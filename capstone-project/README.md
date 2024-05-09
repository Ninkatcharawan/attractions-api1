 
Data Warehouse and Business Intelligence
Capstone Project Documentation
By
Saranporn Kanjanasukhon 65199160202
Atcharawan Kamsai 65199160206



Table of Contents
Introduction	2
Technologies Used:	3
Data Warehouse and Business Intelligence Capstone Project Details	3
1.	Diagram Data Pipeline	3
2.	Workflow Description:	4
3.	Problem	4
4.	Data Modeling	5
5.	Cloud Platform used in this project	6
6.	Data Ingestion and Orchestration:	7
7.	Data Warehouse	8
8.	Data Transformation The original dataset has 47 columns shown in the table	11
9.	Dashboard	14
10.	Instruction of Code in this project	16
Scraping data using API and Airflow taks	17
Cleaning data using Lambda	20
Get data from S3 using Python Script	23












Capstone Project Documentation
Introduction:
In this project, we aim to gather income data from the Thailand government's open data portal (https://data.go.th/dataset/income2) using their provided API (https://opend.data.go.th/register_api/login.php). We will utilize GitHub Codespaces for code development and Airflow for workflow orchestration, deploying them within Docker containers. The collected data will be stored in an AWS S3 bucket, updated quarterly through scheduled Airflow jobs. Additionally, we'll implement a second Airflow job to trigger a Lambda function for data cleaning and storage in a separate S3 bucket. Lastly, we'll establish a connection between the cleaned data in S3 and Power BI for visualization purposes.

Technologies Used:
1.	GitHub Codespaces: Development environment for writing and managing code.
2.	Docker: Containerization technology for deploying Airflow.
3.	Airflow: Workflow orchestration tool for scheduling and managing data tasks.
4.	AWS S3: Cloud storage service for housing both raw and cleaned data.
5.	AWS Lambda: AWS serverless compute service for data processing tasks.
6.	Power BI: Business intelligence tool for data visualization and analysis. 

Project Link on GitHub: https://github.com/Ninkatcharawan/data-warehouse-final-project/tree/a681d68338ea141190ebba905012ba5cbff8b0d8/capstone-project 

Data Warehouse and Business Intelligence Capstone Project Details
1.	Diagram Data Pipeline


 
2.	Workflow Description:
•	Data Scraping and Storage:
	We will develop Python code to scrape data from the provided API.
	Airflow will be configured to run the scraping job periodically, storing the collected data in an S3 bucket on AWS.
•	Data Cleaning and Storage:
	A second Airflow job will be set up to trigger a Lambda function upon data arrival in the S3 bucket.
	The Lambda function will execute data cleaning tasks and save the processed data to a different S3 bucket.
•	Data Visualization:
	Python scripts will facilitate the connection between the cleaned data in S3 and Power BI.
	Power BI will be utilized to create interactive visualizations based on the collected and cleaned data.

3.	Problem
The government, organizations involved in national development and income, as well as the public, must be able to view a summary of the revenue that the government is able to collect each year. This will allow the government to implement various revenue collection policies in a more targeted manner, and citizens can also see the government's performance through the monthly or annual revenue summaries. The government's revenue data must be collected from all agencies that directly generate revenue for the government, including various deductions that occur. Therefore, there should be a good and updatable data collection method in place so that the government's revenue can be summarized monthly or annually in an easily accessible manner.

4.	Data Modeling 
the data modeling or schema for tables related to revenue and expenses in a database.
 
1)	revenue table: 
•	This table stores revenue data for the government or an organization.
•	It has columns for id, government Agency (likely the agency/department generating the revenue), month (a numerical representation of the month), month_name (the name of the month), month_year (the year associated with the month), value (the actual revenue amount), variable (possibly a category or type of revenue), and year.
•	There is also a calculated field total_revenue which would sum up the revenue amounts.
2)	Date table: 
•	This appears to be a separate dimension or lookup table related to dates.
•	It has fields for Date (ldate format), Month (numerical month), Monthnum (another representation of the month number), and Year.
•	This table could be used to join with the revenue and expenses tables to provide additional date-related information or filters.
3)	expenses table: 
•	This table stores expense or deduction data for the government or organization.
•	It has columns for id, deduct (category or type of deduction/expense), month (numerical month), month_name, month_year, value (the expense amount), variable (expense category), and year.
The presence of separate revenue and expenses tables, along with the Date dimension table, this type of data modeling is used in data warehousing, allowing for efficient querying and analysis of revenue and expense data across different dimensions like time, agency/department, and expense/revenue categories.

5.	Cloud Platform used in this project
Amazon Web Services (AWS)
•	Amazon Simple Storage Service (S3) : Use for Collect data
 
Bucker 1: opendatagoth-dw-project, it’s raw data.
 
Bucker 2: opendata-dw-procject-cleaned, it’s data that already cleaned.
 




•	AWS Lambda : Use for Transform data 
 
6.	Data Ingestion and Orchestration:
Workflow Orchestration with Airflow:
To manage the end-to-end data pipeline effectively, we leveraged Apache Airflow for workflow orchestration. Airflow allows us to define, schedule, and monitor data tasks within a directed acyclic graph (DAG) framework. The following components were orchestrated using Airflow:

 

 

7.	Data Warehouse

The dataset in the project exists as a single large table. To improve manageability and query performance, the data is partitioned into two separate storage locations based on specific criteria. This partitioning strategy is a critical step in organizing the dataset for further analysis.
Data Partitioning Process with Python Code on AWS Lambda:
To partition the dataset, Python code deployed on AWS Lambda is employed. This code automates the segmentation of the dataset into two distinct partitions based on predefined criteria. By configuring separate storage locations on AWS S3, each partition is stored independently, ensuring enhanced performance and scalability. This serverless solution efficiently manages data organization, facilitating improved query performance and analysis capabilities.
6.1 Raw Dataset:
 














6.2	Dividing the dataset into two distinct partitions
 
 

•	This is bucket that keep the dataset that be raw dataset.
 
•	These are dataset that we already segmented into two separate tables.
 

 

8.	Data Transformation
The original dataset has 47 columns shown in the table

 
This project uses AWS Lambda to run code that transforms data. The following steps outline the process of converting the data into a format suitable for creating a dashboard.

Step 1 : Delete the specified columns that are not being used.
 

Because it is a summary number of each organization which will be grouped later
Step 2 : Clean Data
	Change the date format to keep only the date without the time, for example, from 1/4/2022 00:00:00 to 1/4/2022.
	Replace NaN values in the DataFrame with 0
	Multiplying all values by 1,000,000 serves to rescale the data, ensuring that it more accurately reflects the intended numerical values or units of measurement
Step 3 : Transform the Data frame from a wide format to a long format
 

Step 4 : Split the data frame into two tables: expenses_df and revenue_df
1) Table expenses_df
o	List of variables for splitting into the expenses_df table:'vat, tax_other, vat_pay_prov, imex_comp, cu_return_dut, vat_deduct'
o	Define a dictionary mapping old variable names to new names
	vat: ภาษีมูลค่าเพิ่ม
	tax_other: ภาษีอื่นๆ
	vat_pay_prov: จัดสรรรายได้จาก VAT ให้ อบจ.
	imex_comp: เงินชดเชยภาษีสำหรับสินค้าส่งออก
	cu_return_dut': อากรถอนคืนกรมศุลกากร
	vat_deduct': หักรายได้จัดสรรจาก VAT ให้ อปท. ตาม พ.ร.บ. กำหนดแผนฯ
o	Add a new column 'deduct' with the value 'deduct'

2) Table revenue_df
o	Filter variables to include rows not present in the table "expenses_df " for the "Revenue_df" table
o	Define a dictionary mapping old variable names to new names
o	Add a new column named 'government agencies' to categorize each variable.
o	Define a dictionary mapping old variable names to new names

 

9.	Dashboard
Data Visualization:
•	Python scripts will facilitate the connection between the cleaned data in S3 and Power BI.
•	Power BI will be utilized to create interactive visualizations based on the collected and cleaned data.
 
Link: Power BI Dashboard Public Web  






10.	Instruction of Code in this project
Scraping data using API and Airflow taks
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import requests
import boto3
from io import StringIO
from airflow.providers.amazon.aws.operators.lambda_function import AwsLambdaInvokeFunctionOperator
import json
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

def load_credentials():
    # Load credentials from JSON file
    with open('/path/to/credentials.json') as f:
        credentials = json.load(f)
    return credentials

def fetch_data_and_upload_to_s3():
    # Function to fetch data and upload to S3
    credentials = load_credentials()

    api_key = credentials['API_KEY']
    aws_access_key_id = credentials['AWS_ACCESS_KEY_ID']
    aws_secret_access_key = credentials['AWS_SECRET_ACCESS_KEY']

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
        'quarterly_lambda_trigger',
        default_args=default_args,
        description='A DAG to trigger Lambda function quarterly',
        schedule_interval='0 10 */3 * *',  # Run at 10:00 AM on the 1st day of every 3rd month
        tags=['DS525']  
    ) as dag:

        # First task scheduled at 10:00 AM
        fetch_data_task = PythonOperator(
            task_id='fetch_data_and_upload_to_s3',
            python_callable=fetch_data_and_upload_to_s3,
        )

        # Second task scheduled at 11:00 AM
        invoke_lambda_function = AwsLambdaInvokeFunctionOperator(
            task_id='setup__invoke_lambda_function',
            function_name='dw-project-nink',
            # payload=SAMPLE_EVENT,
        )

        fetch_data_task >> invoke_lambda_function

    return dag

dag = define_dag()


Cleaning data using Lambda
import pandas as pd
import boto3
from io import StringIO

def lambda_handler(event, context):
    # Create an S3 client
    s3 = boto3.client('s3')

    # Specify the bucket name and file/key name
    bucket_name = 'opendatagoth-dw-project'
    key = 'python-scrape/income_data.csv'

    # Read the file from S3 directly into a DataFrame
    obj = s3.get_object(Bucket=bucket_name, Key=key)
    data = obj['Body'].read().decode('utf-8')
    df = pd.read_csv(StringIO(data))

    # Delete the specified columns
    columns_to_delete = ['rd', 'ex', 'cu', 'dep_other', 'deduct','rd_return','gross_net','net_income','net_net_income']
    df = df.drop(columns=columns_to_delete)

    # Clean 'month_year' to collect only the date component
    df['month_year'] = pd.to_datetime(df['month_year']).dt.date

    # Replace NaN values in the DataFrame with 0
    df = df.fillna(0)

    # Multiply all values by 1000000
    df = df.apply(lambda x: x * 1000000 if x.name not in ['month_year', 'month_name', '_id', 'year', 'month'] else x)

    # Specify the columns you want to keep as identifier variables
    id_vars = ['_id', 'month_year', 'year', 'month', 'month_name']

    # Melt the DataFrame to pivot specified columns into rows
    melted_df = pd.melt(df, id_vars=id_vars, var_name='variable', value_name='value')

    # Define the variables to include in the "Debt" table
    expenses_variables = ['vat', 'tax_other', 'vat_pay_prov', 'imex_comp', 'cu_return_dut', 'vat_deduct']

    # Filter the melted DataFrame to include only the specified variables
    expenses_df = melted_df[melted_df['variable'].isin(expenses_variables)]

    # Define a dictionary mapping old variable names to new names
    variable_mapping = {
        'vat': 'ภาษีมูลค่าเพิ่ม',
        'tax_other': 'ภาษีอื่นๆ',
        'vat_pay_prov': 'จัดสรรรายได้จาก VAT ให้ อบจ.',
        'imex_comp': 'เงินชดเชยภาษีสำหรับสินค้าส่งออก',
        'cu_return_dut': 'อากรถอนคืนกรมศุลกากร',
        'vat_deduct': 'หักรายได้จัดสรรจาก VAT ให้ อปท. ตาม พ.ร.บ. กำหนดแผนฯ'
    }

    # Rename the values in the 'variable' column based on the mapping
    expenses_df.loc[:, 'variable'] = expenses_df['variable'].replace(variable_mapping)

    # Add a new column 'deduct' with the value 'deduct' by creating a new DataFrame
    expenses_df = expenses_df.assign(deduct='deduct')

    # Save the modified DataFrame to a CSV file with UTF-8 encoding directly to S3
    expenses_csv_buffer = StringIO()
    expenses_df.to_csv(expenses_csv_buffer, encoding='utf-8', index=False)
    s3.put_object(Body=expenses_csv_buffer.getvalue(), Bucket='opendata-dw-procject-cleaned', Key='expenses_df.csv')

    # Filter the melted DataFrame to include rows not present in the "Debt" DataFrame for the "Revenue" table
    revenue_df = melted_df[~melted_df.index.isin(expenses_df.index)]

    # Define a mapping of item names to government agencies
    agency_mapping = {
        'rd_pnd': 'กรมสรรพากร',
        'rd_cmp': 'กรมสรรพากร',
        'rd_petrol': 'กรมสรรพากร',
        'rd_vat': 'กรมสรรพากร',
        'rd_buss': 'กรมสรรพากร',
        'rd_fee': 'กรมสรรพากร',
        'rd_other': 'กรมสรรพากร',
        'ex_oil': 'กรมสรรพสามิต',
        'ex_tobac': 'กรมสรรพสามิต',
        'ex_alco': 'กรมสรรพสามิต',
        'ex_beer': 'กรมสรรพสามิต',
        'ex_car': 'กรมสรรพสามิต',
        'ex_drink': 'กรมสรรพสามิต',
        'ex_elect': 'กรมสรรพสามิต',
        'ex_motocy': 'กรมสรรพสามิต',
        'ex_bat': 'กรมสรรพสามิต',
        'ex_telecom': 'กรมสรรพสามิต',
        'ex_other': 'กรมสรรพสามิต',
        'cu_dut_in': 'กรมศุลกากร',
        'cu_duc_out': 'กรมศุลกากร',
        'cu_other': 'กรมศุลกากร',
        'gov_other': 'หน่วยงานอื่น',
        'dep_tressur': 'หน่วยงานอื่น',
        'sell_bond': 'หน่วยงานอื่น',
        'state_entp_chng': 'หน่วยงานอื่น',
        'state_entp': 'รัฐวิสาหกิจ',
        'other':'กรมสรรพสามิต'
    }

    # Add the 'government Agency' column based on the mapping
    revenue_df.loc[:, 'government Agency'] = revenue_df['variable'].map(agency_mapping)

    # Define a dictionary mapping old variable names to new names
    variable_mapping = {
        'rd_pnd': 'ภาษีเงินได้บุคคลธรรมดา',
        'rd_cmp': 'ภาษีเงินได้นิติบุคคล',
        'rd_petrol': 'ภาษีเงินได้ปิโตรเลียม',
        'rd_vat': 'ภาษีมูลค่าเพิ่ม',
        'rd_buss': 'ภาษีธุรกิจเฉพาะ',
        'rd_fee': 'อากรแสตมป์',
        'rd_other': 'รายได้อื่น',
        'ex_oil': 'ภาษีน้ำมันฯ',
        'ex_tobac': 'ภาษียาสูบ',
        'ex_alco': 'ภาษีสุราฯ',
        'ex_beer': 'ภาษีเบียร์',
        'ex_car': 'ภาษีรถยนต์',
        'ex_drink': 'ภาษีเครื่องดื่ม',
        'ex_elect': 'ภาษีเครื่องไฟฟ้า',
        'ex_motocy': 'ภาษีรถจักรยานยนต์',
        'ex_bat': 'ภาษีแบตเตอรี่',
        'ex_telecom': 'ภาษีกิจการโทรคมนาคม',
        'ex_other': 'ภาษีอื่น',
        'other': 'รายได้อื่น',
        'cu_dut_in': 'อากรขาเข้า',
        'cu_dut_out': 'อากรขาออก',
        'cu_other': 'รายได้อื่น',
        'dep_tressur': 'กรมสรรพสามิต',
        'sell_bond': 'หน่วยงานอื่น',
        'state_entp_chng': 'หน่วยงานอื่น',
        'state_entp': 'รัฐวิสาหกิจ',
        'gov_other': 'ส่วนราชการอื่น',
        'cu_duc_out': 'อากรขาออก'
    }

    # Rename the values in the 'variable' column based on the mapping
    revenue_df['variable'] = revenue_df['variable'].replace(variable_mapping)

    # Save the modified DataFrame to a CSV file with UTF-8 encoding directly to S3
    revenue_csv_buffer = StringIO()
    revenue_df.to_csv(revenue_csv_buffer, encoding='utf-8', index=False)
    s3.put_object(Body=revenue_csv_buffer.getvalue(), Bucket='opendata-dw-procject-cleaned', Key='revenue_df.csv')

    return {
        'statusCode': 200,
        'body': 'Data processed and saved to S3'
    }
Get data from S3 using Python Script
1.  Import revenue_df.csv

import pandas as pd
import boto3

# Set up your AWS credentials
session = boto3.Session(
    aws_access_key_id="Access Key",
    aws_secret_access_key="Secret Access Key",
    region_name="ap-southeast-1"
)

# Connect to S3 and retrieve your data file
s3_client = session.client("s3")
response = s3_client.get_object(Bucket="opendata-dw-procject-cleaned", Key="revenue_df.csv")

# Read the data into a Pandas DataFrame
df = pd.read_csv(response["Body"])

2.  Import expenses_df.csv.

import pandas as pd
import boto3

# Set up your AWS credentials
session = boto3.Session(
    aws_access_key_id="Access Key",
    aws_secret_access_key="Secret Access Key",
    region_name="ap-southeast-1"
)

# Connect to S3 and retrieve your data file
s3_client = session.client("s3")
response = s3_client.get_object(Bucket="opendata-dw-procject-cleaned", Key=" expenses_df.csv")

# Read the data into a Pandas DataFrame
df = pd.read_csv(response["Body"])




































