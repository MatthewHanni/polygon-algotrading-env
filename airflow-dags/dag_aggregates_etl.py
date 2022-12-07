from airflow import DAG
from airflow.operators.python import PythonOperator
import datetime
import time
from airflow.models import Variable
import boto3
from sqlalchemy import create_engine

default_args = {
    "owner":"hanni",
    "retries":0,
    "retry_delay":datetime.timedelta(minutes=5)
}

def get_db_connection():
    DATABASE_USER = Variable.get("RDS_INFERNO_DATABASE_USER")
    DATABASE_PASSWORD = Variable.get("RDS_INFERNO_DATABASE_PASSWORD")
    DATABASE_HOST = Variable.get("RDS_INFERNO_DATABASE_HOST")
    DATABASE_PORT = Variable.get("RDS_INFERNO_DATABASE_PORT")
    DATABASE_DBNAME = Variable.get("RDS_INFERNO_DATABASE_DBNAME")
    database_url = f'mysql://{DATABASE_USER}:{DATABASE_PASSWORD}@{DATABASE_HOST}:{DATABASE_PORT}/{DATABASE_DBNAME}'
    engine = create_engine(database_url, echo=True)
    connection = engine.connect()
    return connection

def get_ticker_list(conn):
    ticker_list = []

    ticker_list = ['UPS','SNOW','YUM']
    return ticker_list

    query = """SELECT ticker FROM reference_db.tickers WHERE market = 'stocks'"""
    results = conn.execute(query).fetchall()
    for result in results:
        ticker_list.append(result[0])
    return ticker_list

def get_latest_record_information(ticker,conn):
    try:
        results = conn.execute(
            f'SELECT max(datetime),max(t) FROM aggregates_adjusted.{ticker} order by t desc').fetchall()
        last_entry_date = str(results[0][0].date())
        last_entry_timestamp = str(results[0][1])
        return last_entry_date, last_entry_timestamp
    except:
        return None, None



def process_ticker(ticker, conn,ecs_client):

    cluster = Variable.get("ECS_CLUSTER_POLYGON_ARN")
    task_definition = Variable.get("ECS_CLUSTER_TASK_DEFINITION_AGGREGATES")
    AGGREGATES_API_KEY = Variable.get("POLYGON_AGGREGATES_API_KEY")

    DATABASE_USER = Variable.get("RDS_INFERNO_DATABASE_USER")
    DATABASE_PASSWORD = Variable.get("RDS_INFERNO_DATABASE_PASSWORD")
    DATABASE_HOST = Variable.get("RDS_INFERNO_DATABASE_HOST")
    DATABASE_PORT = Variable.get("RDS_INFERNO_DATABASE_PORT")
    DATABASE_DBNAME = Variable.get("RDS_INFERNO_DATABASE_DBNAME")
    last_entry_date, last_entry_timestamp = get_latest_record_information(ticker=ticker,conn=conn)
    timespan = 'minute'
    multiplier = '5'

    if last_entry_date is not None and last_entry_timestamp is not None:
        overrides = {
            'containerOverrides': [
                {'name': 'aggregates_etl',
                 'environment': [
                     {
                         'name': 'TICKER',
                         'value': ticker
                     }, {
                         'name': 'AGGREGATES_API_KEY',
                         'value': AGGREGATES_API_KEY
                     }, {
                         'name': 'MULTIPLIER',
                         'value': multiplier
                     }, {
                         'name': 'TIMESPAN',
                         'value': timespan
                     }, {
                         'name': 'ADJUSTED',
                         'value': "true"
                     }, {
                         'name': 'DATABASE_USER',
                         'value': DATABASE_USER
                     }, {
                         'name': 'DATABASE_PASSWORD',
                         'value': DATABASE_PASSWORD
                     }, {
                         'name': 'DATABASE_HOST',
                         'value': DATABASE_HOST
                     }, {
                         'name': 'DATABASE_PORT',
                         'value': DATABASE_PORT
                     }, {
                         'name': 'DATABASE_DBNAME',
                         'value': DATABASE_DBNAME
                     }, {
                         'name': 'LAST_ENTRY_DATETIME',
                         'value': last_entry_date
                     }, {
                         'name': 'LAST_ENTRY_TIMESTAMP',
                         'value': last_entry_timestamp
                     },
                 ],

                 },
            ]
        }
    else:
        overrides = {
            'containerOverrides': [
                {'name': 'aggregates_etl',
                 'environment': [
                     {
                         'name': 'TICKER',
                         'value': ticker
                     }, {
                         'name': 'AGGREGATES_API_KEY',
                         'value': AGGREGATES_API_KEY
                     }, {
                         'name': 'MULTIPLIER',
                         'value': multiplier
                     }, {
                         'name': 'TIMESPAN',
                         'value': timespan
                     }, {
                         'name': 'ADJUSTED',
                         'value': "true"
                     }, {
                         'name': 'DATABASE_USER',
                         'value': DATABASE_USER
                     }, {
                         'name': 'DATABASE_PASSWORD',
                         'value': DATABASE_PASSWORD
                     }, {
                         'name': 'DATABASE_HOST',
                         'value': DATABASE_HOST
                     }, {
                         'name': 'DATABASE_PORT',
                         'value': DATABASE_PORT
                     }, {
                         'name': 'DATABASE_DBNAME',
                         'value': DATABASE_DBNAME
                     },
                 ],

                 },
            ]
        }

    print(f'Starting task: {task_definition} {cluster}')
    response = ecs_client.run_task(taskDefinition=task_definition, cluster=cluster, overrides=overrides)

    task_arn = response['tasks'][0]['taskArn']

    while True:
        time.sleep(5)
        print('Checking status')
        response = ecs_client.describe_tasks(tasks=[task_arn], cluster=cluster)
        status = response['tasks'][0]['lastStatus']
        if status.lower() == 'stopped':
            break
    print(status)

def etl():

    print('Making connection to boto3')
    access_key_id = Variable.get("AWS_ACCESS_KEY_ID")
    secret_access_key = Variable.get("AWS_SECRET_ACCESS_KEY")
    region = Variable.get("ECS_CLUSTER_POLYGON_REGION")
    ecs_client = boto3.client('ecs', aws_access_key_id=access_key_id, aws_secret_access_key=secret_access_key,
                          region_name=region)




    print('Connecting to database...')
    conn = get_db_connection()

    print('Querying for stock tickers')
    ticker_list = get_ticker_list(conn=conn)

    for i in range(len(ticker_list)):
        ticker = ticker_list[i]
        print(f'Processing ticker {i+1}:{len(ticker_list)} {ticker}')
        process_ticker(ticker,conn,ecs_client)



    print('DAG complete')



with DAG(
    default_args=default_args,
    dag_id='dag_aggregates',
    description="DAG: ETL for aggregates",
    start_date=datetime.datetime(2022,12,6,16,30,0),
    schedule_interval='@daily'
) as dag:
    etl_task = PythonOperator(
        task_id="etl",
        python_callable=etl,
        op_kwargs={}
    )
    etl_task