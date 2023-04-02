"""Reference Data ETL

This script extracts data from the Polygon.io reference data tickers endpoint.
Extracted data is converted to a Pandas DataFrame.

"""
import helper_functions as hf
import requests
import time
import pandas as pd
import pathlib
import logging

tmp_csv = 'tmp.csv'


def start(endpoint, api_key,s3_client):
    logger = logging.getLogger('logger')
    logger.info(msg=f'Starting reference endpoint collection:{endpoint}')
    config = hf.load_config()
    bucket = config['polygon']['bucket']
    data_folder = config['polygon']['data_folder']
    rate_limit_timeout = float(config['polygon']['rate_limit_timeout'])
    out_key = str(pathlib.PurePosixPath(data_folder, 'reference', endpoint, f'{endpoint}.csv'))
    out_key_timestamped = str(pathlib.PurePosixPath(data_folder, 'reference', endpoint,'timestamped', f'{endpoint}--{hf.get_file_timestamp()}.csv'))





    url = f'https://api.polygon.io/v3/reference/{endpoint}?apiKey={api_key}&limit=1000'
    all_results = []
    while True:
        logger.debug(f'Current cumulative record count:{len(all_results)}. Calling endpoint {url}')
        response = requests.get(url)

        if response.status_code != 200:
            logger.debug(f'reference/{endpoint} endpoint returned non-200 status_code.')
            try:
                logger.debug(f'Status code:{response.status_code}')
                logger.debug(str(response.json()))
            except:
                logger.exception('Invalid status')
                exit()


        response_json = response.json()
        all_results.extend(response_json['results'])

        # If next_url is not included in the response, there is no more pagination required.
        if 'next_url' not in response_json:
            break

        next_url = response_json['next_url']
        url = f'{next_url}&apiKey={api_key}'
        time.sleep(rate_limit_timeout)

    df = pd.DataFrame(all_results)
    df = df.drop_duplicates()
    df.to_csv(tmp_csv, index=False)

    with open(tmp_csv, 'rb') as f:
        s3_client.upload_fileobj(f, Bucket=bucket, Key=out_key)
    with open(tmp_csv, 'rb') as f:
        s3_client.upload_fileobj(f, Bucket=bucket, Key=out_key_timestamped)

    return df
