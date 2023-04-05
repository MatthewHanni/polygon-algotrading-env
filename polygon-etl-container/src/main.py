import reference_endpoints_etl
import aggregates_stocks_etl
import random
import helper_functions as hf
import boto3
import logging
import os

# By default, the debug-level is set to info in the Dockerfile environment variable,
# but a command arg can be passed to override.
debug_level_arg = os.getenv('DEBUG_LEVEL')
debug_level_arg = 'DEBUG'
if debug_level_arg.upper() == 'INFO':
    debug_level = logging.INFO
else:
    debug_level = logging.DEBUG

# Set up our logger. Docker is configured to send all logs to AWS Cloudwatch
logger = logging.getLogger('logger')
logger.setLevel(debug_level)
formatter = logging.Formatter('%(asctime)s %(levelname)s\t%(message)s -|- %(module)s>%(funcName)s')
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)

logger.info('Debug level set:%s', debug_level)

api_key = hf.get_api_key()
s3_client = boto3.client('s3')

reference_endpoints_etl.start(endpoint='dividends', api_key=api_key, s3_client=s3_client)
reference_endpoints_etl.start(endpoint='splits', api_key=api_key, s3_client=s3_client)
df_tickers = reference_endpoints_etl.start(endpoint='tickers', api_key=api_key, s3_client=s3_client)
df_stocks = df_tickers[df_tickers['market'] == 'stocks'].copy()
tickers = df_stocks['ticker'].unique().tolist()
random.shuffle(tickers)

ticker_count = 0
num_tickers = len(tickers)
for ticker in tickers:
    ticker_count += 1
    logger.info('Stock Aggregates Status: %s:%s %s', ticker_count, num_tickers, ticker)
    aggregates_stocks_etl.start(ticker=ticker, adjusted=True, api_key=api_key, s3_client=s3_client)
    aggregates_stocks_etl.start(ticker=ticker, adjusted=False, api_key=api_key, s3_client=s3_client)
