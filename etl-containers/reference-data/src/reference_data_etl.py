import datetime
import requests
import time
import pandas as pd
import os
from sqlalchemy import create_engine
import pytz

RATE_LIMIT_TIMEOUT = .1

endpoint = os.environ.get('ENDPOINT').lower()
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

    if response.status_code != 200:
        exit(-1)

    response_json = response.json()

    all_results.extend(response_json['results'])
    if 'next_url' not in response_json:
        break

    next_url = response_json['next_url']
    url = f'{next_url}&apiKey={api_key}'
    time.sleep(RATE_LIMIT_TIMEOUT)

df = pd.DataFrame(all_results)

database_url = f'mysql://{database_user}:{database_password}@{database_host}:{database_port}/{database_name}'

engine = create_engine(database_url, echo=True)
connection = engine.connect()

df['timestamp'] = datetime.datetime.now(pytz.timezone('US/Eastern'))
df.to_sql(endpoint, con=engine, if_exists='replace')
