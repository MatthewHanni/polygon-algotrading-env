"""Aggregates Data ETL DAG

This DAG orchestrates the aggregates ETL container task.
This task cycles through all stock tickers stored in the project RDS database

"""

from airflow import DAG
from airflow.operators.python import PythonOperator
import datetime
import time
from airflow.models import Variable
import boto3
from sqlalchemy import create_engine
import json

default_args = {
    "owner": "hanni",
    "retries": 0,
    "retry_delay": datetime.timedelta(minutes=5)
}


def get_db_connection():
    """Gets and prints the spreadsheet's header columns

    Returns:
        connection: A SqlAlchemy database connection
"""

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
    """Retrieve a list of stock tickers from project database. Truncated to 128 for proof of concept.

    Args:
        connection (sqlalchemy.engine.base.Engine): A SqlAlchemy database connection

    Returns:
        ticker_list: a list of stock tickers
    """

    ticker_list = []

    query = """SELECT ticker FROM reference_db.tickers WHERE market = 'stocks'"""
    results = conn.execute(query).fetchall()
    for result in results:
        ticker_list.append(result[0])

    ticker_list = ticker_list[:128]

    return ticker_list


def get_most_recent_key(ticker, s3_client):

    """Retrieve a list of stock tickers from project database. Truncated to 128 for proof of concept.

    Args:
        ticker (str): stock ticker
        s3_client (boto3.session.BaseClient): boto3 client connection to S3

    Returns:
        max_key: the S3 key associated with the object most recently uploaded tagged to that ticker. None, otherwise.
    """


    database_name = 'aggregates_adjusted'
    print(f'Looking for latest record for {ticker}')


    prefix = f'{database_name}/{ticker}/'
    print(f'Prefix set:{prefix}')
    all_keys = get_key_list(s3_client=s3_client, prefix=prefix)
    if len(all_keys) == 0:
        return None
    max_date = None
    max_key = None
    for key_object in all_keys:
        last_modified = key_object['LastModified']
        key = key_object['Key']
        if max_date is None or last_modified > max_date:
            max_date = last_modified
            max_key = key
    print(f'Latest key:{max_key}\tLatest date:{max_date}')
    return max_key


def get_key_list(s3_client, prefix):
    """Returns a list of object keys associated with the given ticker's aggregate data

    Args:
        s3_client (boto3.session.BaseClient): boto3 client connection to S3
        prefix (str): the stock ticker and other organizational information which create a logically unique entity.

    Returns:
        all_keys: A list of S3 objects which match the prefix. A blank list, otherwise.
    """
    all_keys = []
    next_continuation_token = None
    bucket_name = Variable.get("AGGREGATES_BUCKET")
    while True:
        print('Looking for keys...')
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix) if next_continuation_token is None \
            else s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix, ContinuationToken=next_continuation_token)
        if 'Contents' in response:
            all_keys.extend(response['Contents'])
            print(f'Found {len(response["Contents"])} keys (Total:{len(all_keys)})')

        if 'NextContinuationToken' in response:
            next_continuation_token = response['NextContinuationToken']
            print(next_continuation_token)
        else:
            break
    print(f'Total keys found {len(all_keys)}')
    return all_keys


def get_latest_key_record_offsets(s3_client, key):

    """Returns the latest timestamp of observed data from the most recently uploaded file for the ticker.

    Args:
        s3_client (boto3.session.BaseClient): boto3 client connection to S3
        key (str): The S3 object key of the ticker's most recently uploaded file

    Returns:
        latest_entry_timestamp: The latest observation for a Unix Msec timestamp for the start of the aggregate window
    """
    bucket_name = Variable.get("AGGREGATES_BUCKET")
    s3_client.download_file(bucket_name, key, 'tmp.json')
    print('Downloaded key')
    latest_entry_timestamp = None
    with open('tmp.json') as f:
        data = json.load(f)
        for record in data['results']:
            t = record['t']
            if latest_entry_timestamp is None or t > latest_entry_timestamp:
                latest_entry_timestamp = t
    latest_entry_timestamp = str(latest_entry_timestamp)
    return latest_entry_timestamp


def get_latest_record_information(ticker):
    """Calls helper functions to determine the latest file object we've ingested for this ticker (if any)
    and the latest record's timestamp. This is needed to incrementally retrieve data from Polygon's endpoint.

    Args:
        s3_client (boto3.session.BaseClient): boto3 client connection to S3
        key (str): The S3 object key of the ticker's most recently uploaded file

    Returns:
        latest_entry_timestamp: The latest observation for a Unix Msec timestamp for the start of the aggregate window
    """
    bucket_name = Variable.get("AGGREGATES_BUCKET")
    aws_access_key_id = Variable.get("AWS_ACCESS_KEY_ID")
    aws_secret_access_key = Variable.get("AWS_SECRET_ACCESS_KEY")
    region_name = Variable.get("AWS_REGION_NAME")
    print('Making connection to S3')
    s3_client = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key,
                             region_name=region_name)
    most_recent_key = get_most_recent_key(ticker, s3_client)
    if most_recent_key is None:
        return None

    latest_entry_timestamp = get_latest_key_record_offsets(s3_client=s3_client, bucket_name=bucket_name,
                                                           key=most_recent_key)

    print(f'Most recent entry timestamp{latest_entry_timestamp}')
    return latest_entry_timestamp


# def get_latest_record_information_db(ticker, conn):
#     try:
#         results = conn.execute(
#             f'SELECT max(datetime),max(t) FROM aggregates_adjusted.{ticker} order by t desc').fetchall()
#
#         last_entry_timestamp = str(results[0][1])
#         return last_entry_timestamp
#     except:
#         return None


def process_ticker(ticker, conn, ecs_client):

    """   Makes a connection to ECS via boto3 and creates a task based on the task definition specified in the Airflow secrets.

    Args:
        ecs_client (boto3.session.BaseClient): boto3 client connection to ec2
        ticker (str): Stock ticker
        conn (sqlalchemy.engine.base.Engine): A SqlAlchemy database connection


    """
    cluster = Variable.get("ECS_CLUSTER_POLYGON_ARN")
    task_definition = Variable.get("ECS_CLUSTER_TASK_DEFINITION_AGGREGATES")
    AGGREGATES_API_KEY = Variable.get("POLYGON_AGGREGATES_API_KEY")

    DATABASE_USER = Variable.get("RDS_INFERNO_DATABASE_USER")
    DATABASE_PASSWORD = Variable.get("RDS_INFERNO_DATABASE_PASSWORD")
    DATABASE_HOST = Variable.get("RDS_INFERNO_DATABASE_HOST")
    DATABASE_PORT = Variable.get("RDS_INFERNO_DATABASE_PORT")
    DATABASE_DBNAME = Variable.get("RDS_INFERNO_DATABASE_DBNAME")

    aws_access_key_id = Variable.get("AWS_ACCESS_KEY_ID")
    aws_secret_access_key = Variable.get("AWS_SECRET_ACCESS_KEY")
    region_name = Variable.get("AWS_REGION_NAME")
    bucket_name = Variable.get("AGGREGATES_BUCKET")

    timespan = 'minute'
    multiplier = '5'

    latest_entry_timestamp = get_latest_record_information(ticker=ticker, conn=conn)

    if latest_entry_timestamp is not None:
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
                         'name': 'LATEST_ENTRY_TIMESTAMP',
                         'value': latest_entry_timestamp
                     }, {
                         'name': 'AWS_ACCESS_KEY',
                         'value': aws_access_key_id
                     }, {
                         'name': 'AWS_SECRET_KEY',
                         'value': aws_secret_access_key
                     }, {
                         'name': 'DATABRICKS_BUCKET',
                         'value': bucket_name
                     }, {
                         'name': 'DATABRICKS_BUCKET_REGION',
                         'value': region_name
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
                     }, {
                         'name': 'AWS_ACCESS_KEY',
                         'value': aws_access_key_id
                     }, {
                         'name': 'AWS_SECRET_KEY',
                         'value': aws_secret_access_key
                     }, {
                         'name': 'DATABRICKS_BUCKET',
                         'value': bucket_name
                     }, {
                         'name': 'DATABRICKS_BUCKET_REGION',
                         'value': region_name
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
        print(f'Processing ticker {i + 1}:{len(ticker_list)} {ticker}')
        process_ticker(ticker, conn, ecs_client)

    print('DAG complete')


with DAG(
        default_args=default_args,
        dag_id='dag_aggregates',
        description="DAG: ETL for aggregates",
        start_date=datetime.datetime(2022, 12, 6, 16, 30, 0),
        schedule_interval='@daily'
) as dag:
    etl_task = PythonOperator(
        task_id="etl",
        python_callable=etl,
        provide_context=True
    )
    etl_task