"""Reference Data ETL - Splits

This DAG orchestrates the reference-data ETL container task.
Extracted data is converted to a Pandas DataFrame and overwrites existing data in a MySQL table.


"""

from airflow import DAG
from airflow.operators.python import PythonOperator
import datetime
from airflow.models import Variable
import boto3

default_args = {
    "owner":"hanni",
    "retries":3,
    "retry_delay":datetime.timedelta(minutes=5)
}

def etl():
    '''
        Makes a connection to ECS via boto3 and
        creates a task based on the task definition specified in the Airflow secrets.

                Parameters:
                        None
                Returns:
                        None
        '''
    print('Assigning variables')
    endpoint = 'splits'
    access_key_id = Variable.get("AWS_ACCESS_KEY_ID")
    secret_access_key = Variable.get("AWS_SECRET_ACCESS_KEY")
    region = Variable.get("ECS_CLUSTER_POLYGON_REGION")
    cluster = Variable.get("ECS_CLUSTER_POLYGON_ARN")
    task_definition = Variable.get("ECS_CLUSTER_TASK_DEFINITION_REFERENCE_DATA")
    TICKERS_API_KEY = Variable.get("POLYGON_TICKERS_API_KEY")
    DIVIDENDS_API_KEY = Variable.get("POLYGON_DIVIDENDS_API_KEY")
    SPLITS_API_KEY = Variable.get("POLYGON_SPLITS_API_KEY")
    DATABASE_USER = Variable.get("RDS_INFERNO_DATABASE_USER")
    DATABASE_PASSWORD = Variable.get("RDS_INFERNO_DATABASE_PASSWORD")
    DATABASE_HOST = Variable.get("RDS_INFERNO_DATABASE_HOST")
    DATABASE_PORT = Variable.get("RDS_INFERNO_DATABASE_PORT")
    DATABASE_DBNAME = Variable.get("RDS_INFERNO_DATABASE_DBNAME")


    print('Making connection to boto3')
    client = boto3.client('ecs',aws_access_key_id = access_key_id,aws_secret_access_key=secret_access_key,region_name=region)
    overrides = {
            'containerOverrides': [
                {'name': 'reference_data_etl',
                    'environment': [
                        {
                            'name': 'ENDPOINT',
                            'value': endpoint
                        },                    {
                            'name': 'TICKERS_API_KEY',
                            'value': TICKERS_API_KEY
                        },                    {
                            'name': 'DIVIDENDS_API_KEY',
                            'value': DIVIDENDS_API_KEY
                        },                    {
                            'name': 'SPLITS_API_KEY',
                            'value': SPLITS_API_KEY
                        },                    {
                            'name': 'DATABASE_USER',
                            'value': DATABASE_USER
                        },                    {
                            'name': 'DATABASE_PASSWORD',
                            'value': DATABASE_PASSWORD
                        },                    {
                            'name': 'DATABASE_HOST',
                            'value': DATABASE_HOST
                        },                    {
                            'name': 'DATABASE_PORT',
                            'value': DATABASE_PORT
                        },                    {
                            'name': 'DATABASE_DBNAME',
                            'value': DATABASE_DBNAME
                        },
                    ],

        },
        ]
    }
    print('Running task')
    response = client.run_task(taskDefinition=task_definition, cluster=cluster, overrides=overrides)
    print(response)


with DAG(
    default_args=default_args,
    dag_id='dag_reference_data_etl_splits',
    description="DAG: ETL for reference data/splits",
    start_date=datetime.datetime(2022,11,28,8,0,0),
    schedule_interval='@daily'
) as dag:
    etl_task = PythonOperator(
        task_id="etl",
        python_callable=etl,
        op_kwargs={}
    )
    etl_task