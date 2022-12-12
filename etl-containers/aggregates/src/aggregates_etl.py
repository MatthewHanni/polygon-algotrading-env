"""Aggregates ETL

This script extracts data from the Polygon.io aggregates endpoint.
The aggregates endpoint provides aggregated candles for a given ticker.
Extracted JSON data is written to an S3 bucket.

The intended manner of execution for this script is via containerised compute, such as Amazon ECS.
Parameters including API keys, database credentials, and endpoint parameters are provided as ENV arguments.

"""


import datetime
import requests
import time
import pandas as pd
import os
import pytz
from sqlalchemy import create_engine
import json
import boto3
import random
import string

def ts_to_datetime(ts) -> datetime:
    '''
            Returns a naive datetime object representation of an epoch timestamp .

                    Parameters:
                            ts (int): The Unix Msec timestamp for the start of the aggregate window.
                    Returns:
                            dt (str): datetime representation of ts
    '''
    # TODO we also need to convert this eastern time. It seems to do so automatically
    dt = datetime.datetime.fromtimestamp(ts / 1000.0)
    return dt


def get_nonce(length=5):
    '''
        Returns a random 5-digit string. Used to prevent (significantly reduce) naming collisions.

                Parameters:
                        length (int): Desired nonce length
                Returns:
                        result_str (str): randomized uppercase+digits nonce of the length specified in the arguments
        '''
    letters = string.ascii_uppercase + string.digits
    result_str = ''.join(random.choice(letters) for i in range(length))
    return result_str



DEFAULT_START_DATE = datetime.datetime(year=1990, month=1, day=1)
RATE_LIMIT_TIMEOUT = 1

ticker = os.environ.get('TICKER')
api_key = os.environ.get(f'AGGREGATES_API_KEY')
database_user = os.environ.get('DATABASE_USER')
database_password = os.environ.get('DATABASE_PASSWORD')
database_host = os.environ.get('DATABASE_HOST')
database_port = os.environ.get('DATABASE_PORT')
multiplier = os.environ.get('MULTIPLIER')
timespan = os.environ.get('TIMESPAN')

latest_entry_timestamp = os.environ.get('LATEST_ENTRY_TIMESTAMP')
adjusted = os.environ.get('ADJUSTED')
aws_access_key_id = os.environ.get('AWS_ACCESS_KEY')
aws_secret_access_key = os.environ.get('AWS_SECRET_KEY')
bucket_name = os.environ.get('DATABRICKS_BUCKET')
region_name = os.environ.get('DATABRICKS_BUCKET_REGION')
tmp_path = 'tmp.json'

s3_client = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key,
                         region_name=region_name)

limit = 50000

if adjusted == 'true':
    database_name = 'aggregates_adjusted'
elif adjusted == 'false':
    database_name = 'aggregates_actual'
else:
    print(f'Unknown adjusted type:{type(adjusted)}\tValue:{adjusted}')
    exit()



if latest_entry_timestamp is None:
    # If we have not yet recorded data for this ticker, start from the predefined date
    from_dt = DEFAULT_START_DATE
else:
    # If we have collected data,
    latest_entry_timestamp = int(latest_entry_timestamp)


    from_dt = ts_to_datetime(latest_entry_timestamp)

table_name = ticker.upper()




to_ = str(datetime.date.today())
all_results = []
from_ = str(from_dt.date())
while True:


    url = f'https://api.polygon.io/v2/aggs/ticker/{ticker}/range/{multiplier}/{timespan}/{from_}/{to_}?adjusted={adjusted}&sort=asc&limit={limit}&apiKey={api_key}'

    response = requests.get(url)

    if response.status_code != 200:
        exit(-1)

    response_json = response.json()

    with open(tmp_path, 'w') as f:
        json.dump(response_json, f)

    with open(tmp_path, 'rb') as f:
        timestamp = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
        s3_path = f'{database_name}/{ticker}/{ticker}--{timestamp}--{get_nonce()}.json'
        s3_client.upload_fileobj(f, bucket_name, s3_path)

    prev_count = -1
    for i in range(len(response_json['results'])):
        t = response_json['results'][i]['t']

        # Check if the max timestamp in our DB is greater than this result record. If so, we've likely already captured it.
        if latest_entry_timestamp is not None and latest_entry_timestamp >= t:
            continue

        # Check if the latest record in memory is greater than this result record. If so, we've likely already captured it.
        if len(all_results) > 0 and all_results[-1]['t'] >= t:
            continue

        response_json['results'][i]['datetime'] = ts_to_datetime(t)
        all_results.append(response_json['results'][i])

    # # This assumption is incorrect. The API does not work this way.
    # ERROR: If the number of results returned was less than the max number of values we expect back, then we are up-to-date
    # if len(response_json['results']) < limit:
    #    break

    # If we've returned 0 results, we are up-to-date
    if response_json['resultsCount'] == 0:
        break

    # If we haven't added any new records
    if prev_count == len(all_results):
        break

    prev_count = len(all_results)

    latest_result_timestamp = response_json['results'][-1]['t']
    latest_result_datetime = ts_to_datetime(latest_result_timestamp)
    latest_result_date = str(latest_result_datetime).split(' ')[0]


    if from_ == latest_result_date:
        break
    else:
        from_ = latest_result_date

    time.sleep(RATE_LIMIT_TIMEOUT)

# df = pd.DataFrame(all_results)

# database_url = f'mysql://{database_user}:{database_password}@{database_host}:{database_port}/{database_name}'
# engine = create_engine(database_url, echo=True)
# connection = engine.connect()

# df['insert_timestamp'] = datetime.datetime.now(pytz.timezone('US/Eastern'))

# df.to_sql(table_name, con=engine, if_exists='append')
