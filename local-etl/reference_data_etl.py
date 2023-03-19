"""Reference Data ETL

This script extracts data from the Polygon.io reference data endpoints.
Extracted data is converted to a Pandas DataFrame.

"""

import helper_functions as hf
import datetime
import requests
import time
import pandas as pd
import os
import pytz
import logging

# Polygon suggests staying under 100 requests per second.
# I conservatively set my application's limit to a tenth of a second.
RATE_LIMIT_TIMEOUT = .1

api_key = hf.get_api_key()

response = None

url = f'https://api.polygon.io/v3/reference/tickers?apiKey={api_key}&limit=1000'
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

# Create a record timestamp to indicate when this data was obtained.
df['timestamp'] = datetime.datetime.now(pytz.timezone('US/Eastern'))

