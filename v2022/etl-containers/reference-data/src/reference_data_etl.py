"""Reference Data ETL

This script extracts data from the Polygon.io reference data endpoints.
Extracted data is converted to a Pandas DataFrame and overwrites existing data in a MySQL table.

The intended manner of execution for this script is via containerised compute, such as Amazon ECS.
Parameters including API keys, database credentials, and indication of the specific reference data
endpoint are provided as ENV arguments.

"""


import datetime
import requests
import time
import pandas as pd
import os
from sqlalchemy import create_engine
import pytz

# Polygon suggests staying under 100 requests per second.
# I conservatively set my application's limit to a tenth of a second.
RATE_LIMIT_TIMEOUT = .1

endpoint = os.environ.get('ENDPOINT').lower()
# I've created different API keys for each endpoint to improve robustness and make potential troubleshooting easier
api_key = os.environ.get(f'{endpoint.upper()}_API_KEY')
database_user = os.environ.get('DATABASE_USER')
database_password = os.environ.get('DATABASE_PASSWORD')
database_host = os.environ.get('DATABASE_HOST')
database_port = os.environ.get('DATABASE_PORT')
database_name = os.environ.get('DATABASE_DBNAME')

response = None

url = f'https://api.polygon.io/v3/reference/{endpoint}?apiKey={api_key}&limit=1000'
all_results = []
while True:
    response = requests.get(url)

    #TODO Alert to this error
    if response.status_code != 200:
        exit(-1)

    response_json = response.json()

    all_results.extend(response_json['results'])

    # If next_url is not included in the response, there is no more pagination required - we've saw all there is to see.
    if 'next_url' not in response_json:
        break

    _next_url = response_json['next_url']
    url = f'{_next_url}&apiKey={api_key}'
    time.sleep(RATE_LIMIT_TIMEOUT)


df = pd.DataFrame(all_results)

database_url = f'mysql://{database_user}:{database_password}@{database_host}:{database_port}/{database_name}'

engine = create_engine(database_url, echo=True)
connection = engine.connect()

# Create a record timestamp to indicate when this data was obtained.
df['timestamp'] = datetime.datetime.now(pytz.timezone('US/Eastern'))
df.to_sql(endpoint, con=engine, if_exists='replace')
