import reference_endpoints_etl
import aggregates_stocks_etl
import random
import helper_functions as hf
import boto3

api_key = hf.get_api_key()
s3_client = boto3.client('s3')
reference_endpoints_etl.start(endpoint='dividends', api_key=api_key,s3_client=s3_client)
reference_endpoints_etl.start(endpoint='splits', api_key=api_key,s3_client=s3_client)
df_tickers = reference_endpoints_etl.start(endpoint='tickers', api_key=api_key,s3_client=s3_client)
df_stocks = df_tickers[df_tickers['market'] == 'stocks'].copy()
tickers = df_stocks['ticker'].unique().tolist()
random.shuffle(tickers)

ticker_count = 0
for ticker in tickers:
    ticker_count += 1
    hf.print_log(f'Stock Aggregates Status: {ticker_count}:{len(tickers)} {ticker}')
    aggregates_stocks_etl.start(ticker=ticker, adjusted=True, api_key=api_key,s3_client=s3_client)
    aggregates_stocks_etl.start(ticker=ticker, adjusted=False, api_key=api_key,s3_client=s3_client)
