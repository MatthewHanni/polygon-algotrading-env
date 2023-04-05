"""Aggregates ETL

This script extracts data from the Polygon.io aggregates endpoint.
The aggregates endpoint provides aggregated candles for a given ticker.
"""

import datetime
import requests
import time
import pathlib
import pandas as pd
from helper_functions import load_config
import logging

DEFAULT_START_DATE = datetime.datetime(year=1976, month=1, day=1)

LIMIT = 50000
MULTIPLIER = '1'
TIMESPAN = 'minute'
tmp_json = 'tmp.json'
tmp_csv = 'tmp.csv'


def start(ticker, adjusted, api_key, s3_client):
    logger = logging.getLogger('logger')
    config = load_config()
    bucket = config['polygon']['bucket']
    data_folder = config['polygon']['data_folder']
    rate_limit_timeout = float(config['polygon']['rate_limit_timeout'])

    ticker = ticker.upper()

    if adjusted:
        adjusted = 'true'
        adjusted_key_descriptor = 'adjusted'
    elif not adjusted:
        adjusted = 'false'
        adjusted_key_descriptor = 'raw'
    else:
        logger.error('Invalid adjusted parameter #%s#',adjusted)
        exit()

    out_key = str(pathlib.PurePosixPath(data_folder, 'aggregates', ticker, f'{ticker}--{adjusted_key_descriptor}.csv'))
    to_ = str(datetime.date.today())
    from_ = str(DEFAULT_START_DATE.date())
    all_results = []
    while True:

        url = f'https://api.polygon.io/v2/aggs/ticker/{ticker}/range/{MULTIPLIER}/{TIMESPAN}/{from_}/{to_}?adjusted={adjusted}&sort=asc&limit={LIMIT}&apiKey={api_key}'
        debug_url = url.replace(api_key, 'xXx_API_KEY_xXx')
        logger.debug('Querying %s', debug_url)
        time.sleep(rate_limit_timeout)
        response = requests.get(url)

        if response.status_code != 200:
            logger.debug('aggregates endpoint returned non-200 status_code: %s', response.status_code)
            break
        try:
            response_json = response.json()
            if 'results' in response_json:
                all_results.extend(response_json['results'])
        except:
            logger.critical('Could not parse JSON response')
            break

        # If we've returned 0 results, we are up-to-date
        if response_json['resultsCount'] == 0:
            logger.debug('Returned no results')
            break

        # Check for 0 results first
        latest_timestamp = response_json['results'][-1]['t']

        if len(response_json['results']) < LIMIT:
            logger.debug('Query did not return up to the limit')
            break

        latest_result_date = datetime.datetime.fromtimestamp(latest_timestamp / 1000.0).date()

        if to_ == latest_result_date:
            logger.debug('Latest date reached:%s', to_)
            break

        from_ = latest_result_date
    if len(all_results) == 0:
        return

    df = pd.DataFrame(all_results)
    df = df.drop_duplicates()
    df.to_csv(tmp_csv, index=False)

    with open(tmp_csv, 'rb') as f:
        s3_client.upload_fileobj(f, Bucket=bucket, Key=out_key)
