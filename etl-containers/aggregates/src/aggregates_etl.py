import datetime
import requests
import time
import pandas as pd
import os
import pytz
from sqlalchemy import create_engine

DEFAULT_START_DATE = '1990-01-01'
RATE_LIMIT_TIMEOUT = .1

ticker = os.environ.get('TICKER')
api_key = os.environ.get(f'AGGREGATES_API_KEY')
database_user = os.environ.get('DATABASE_USER')
database_password = os.environ.get('DATABASE_PASSWORD')
database_host = os.environ.get('DATABASE_HOST')
database_port = os.environ.get('DATABASE_PORT')
multiplier = os.environ.get('MULTIPLIER')
timespan = os.environ.get('TIMESPAN')
last_entry_datetime = os.environ.get('LAST_ENTRY_DATETIME')
last_entry_timestamp = os.environ.get('LAST_ENTRY_TIMESTAMP')
adjusted = os.environ.get('ADJUSTED')

limit = 50000


if adjusted == 'true':
    database_name = 'aggregates_adjusted'
elif adjusted == 'false':
    database_name = 'aggregates_actual'
else:
    print(f'Unknown adjusted type:{type(adjusted)}\tValue:{adjusted}')
    exit()

if last_entry_datetime is None:
    from_ = DEFAULT_START_DATE
else:
    from_ = str(last_entry_datetime.date())

to_ = datetime.date.today()
table_name = ticker.upper()


def ts_to_datetime(ts) -> str:
    x = datetime.datetime.fromtimestamp(ts / 1000.0)
    return x


all_results = []
while True:

    url = f'https://api.polygon.io/v2/aggs/ticker/{ticker}/range/{multiplier}/{timespan}/{from_}/{to_}?adjusted={adjusted}&sort=asc&limit={limit}&apiKey={api_key}'
    response = requests.get(url)

    if response.status_code != 200:
        exit(-1)

    response_json = response.json()

    for i in range(len(response_json['results'])):
        t = response_json['results'][i]['t']

        # Check if the max timestamp in our DB is greater than this result record. If so, we've likely already captured it.
        if last_entry_timestamp is not None and last_entry_timestamp >= t:
            continue

        # Check if the latest record in memory is greater than this result record. If so, we've likely already captured it.
        if len(all_results) > 0 and all_results[-1]['t'] >= t:
            continue

        response_json['results'][i]['datetime'] = ts_to_datetime(t)
        all_results.append(response_json['results'][i])

    # If the number of results returned was less than the max number of values we expect back, then we are up-to-date
    if len(response_json['results']) < limit:
        break

    latest_result_timestamp = response_json['results'][-1]['t']
    latest_result_datetime = ts_to_datetime(latest_result_timestamp)

    from_ = str(latest_result_datetime).split(' ')[0]
    time.sleep(RATE_LIMIT_TIMEOUT)

df = pd.DataFrame(all_results)

database_url = f'mysql://{database_user}:{database_password}@{database_host}:{database_port}/{database_name}'
engine = create_engine(database_url, echo=True)
connection = engine.connect()

df['timestamp'] = datetime.datetime.now(pytz.timezone('US/Eastern'))

df.to_sql(table_name, con=engine, if_exists='append')
