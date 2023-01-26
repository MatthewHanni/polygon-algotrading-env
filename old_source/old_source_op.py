import configparser
import os
from polygon import RESTClient
import datetime
import pandas as pd
import requests
import logging
import random

adjusted_values = ['true', 'false']
UNIVERSAL_START_DATE = '2000-01-01'
UNIVERSAL_END_DATE = '2142-01-01'

config_path = r'config.conf'
AGGS_LIMIT = 50000
API_KEY = None
DEFAULT_SORT = 'asc'
period_list = [('minute', 1), ('minute', 5), ('minute', 15), ('minute', 30), ('hour', 1), ('hour', 24)]


# Log configuration file elements, as stored in outer-passage.conf in local directory by default.
def get_config_elements():
    config = configparser.ConfigParser()
    config.read(config_path)
    key = config['DEFAULT']['key']
    dest_path = config['DEFAULT']['dest_path']
    log_path = config['DEFAULT']['log_path']
    return key, dest_path, log_path


# Retrieve a list of stock tickers from Polygon
def get_all_tickers(client, ticker_only=True):
    next_url = None
    all_tickers = []

    while True:
        resp = client.reference_tickers_v3(market='stocks') if next_url is None else client.reference_tickers_v3(
            next_url=next_url, market='stocks')
        all_tickers.extend(resp.results)

        try:
            next_url = resp.next_url
        except:
            break
        logging.debug(f'Found tickers: {len(all_tickers)} {next_url}')

    if ticker_only:
        logging.debug(f'Extracting ticker from {len(all_tickers)} ticker objects')
        all_tickers = [result['ticker'] for result in all_tickers]

    return all_tickers


# Retrieve the bars for a specific window. Will return the max number of bars from the from date
def _get_bars(ticker, from_date, to_date, timespan, multiplier, adjusted, client=None):
    logging.debug(
        f'<_get_bars>\tticker:{ticker},multiplier:{multiplier},timespan:{timespan},from_date:{from_date},to_date:{to_date},adjusted:{adjusted}')

    query = f"""https://api.polygon.io/v2/aggs/ticker/{ticker}/range/{multiplier}/{timespan}/{from_date}/{to_date}?adjusted={adjusted}&sort={DEFAULT_SORT}&limit={AGGS_LIMIT}&apiKey={API_KEY}"""

    resp = requests.get(query)

    logging.debug(f'<_get_bars>\tQuerying {query}')
    if resp.status_code != 200:
        logging.error(f'<_get_bars>\tReponse code: {resp.status_code}')
        return

    try:
        j = resp.json()
    except:
        logging.error(f'<_get_bars>\tCould not parse json from result')
        return

    if 'results' not in j:
        logging.error(f'<_get_bars>\tkey "results" not in json')
        return
    results = j['results']
    logging.debug(f'<_get_bars>\tresult length:{len(results)}')

    for result in results:
        result["datetime"] = ts_to_datetime(result['t'])

    return results


def get_aggregate_bars(ticker, from_date, to_date, timespan, multiplier, adjusted, client=None):
    all_results = []
    logging.debug(
        f'<get_aggregate_bars>\tticker:{ticker},multiplier:{multiplier},timespan:{timespan},from_date:{from_date},to_date:{to_date},adjsuted:{adjusted}')

    while str(from_date) < str(datetime.date.today()):
        results = _get_bars(ticker=ticker, from_date=from_date, to_date=to_date, multiplier=multiplier,
                            timespan=timespan, adjusted=adjusted)
        if results is None:
            break
        all_results.extend(results)

        new_from_date = results[-1]['datetime'].date()

        if from_date == new_from_date:
            break
        else:
            from_date = new_from_date

    df = pd.DataFrame(all_results)
    pre_dup_len = len(df)
    df = df.drop_duplicates()
    logging.debug(
        f'<get_aggregate_bars>\tRetrieved total rows:{len(df)}. Dropped {pre_dup_len - len(df)} duplicates')
    return df


def ts_to_datetime(ts) -> str:
    x = datetime.datetime.fromtimestamp(ts / 1000.0)
    return x


# Helper function to find the first day where polygon has data for a given ticker
def query_polygon_first_date(ticker, adjusted, client=None):
    results = _get_bars(ticker=ticker, from_date=UNIVERSAL_START_DATE, to_date=UNIVERSAL_END_DATE, multiplier=1,
                        timespan='day', adjusted=adjusted)
    if results is None:
        return None
    first_date = results[0]['datetime'].date()
    return first_date


# For an existing file, read it and return the largest datetime, else None (if file exists but is empty)
def get_most_recent_record_datetime(file_path):
    df = pd.read_csv(file_path, parse_dates=['datetime'])
    if len(df) == 0:
        return None
    most_recent_timestamp = df['datetime'].max()
    most_recent_datetime = most_recent_timestamp.to_pydatetime()

    logging.debug(f'<get_most_recent_record_datetime>\t{most_recent_datetime}')
    return most_recent_datetime


def get_file_timestamp():
    return datetime.datetime.now().strftime("%Y%m%d-%H%M%S")


def process_ticker(ticker, timespan, multiplier, adjusted, destination_dir_path, client=None):
    logging.debug(f'{ticker}:{timespan}\t{multiplier}')
    _out_file_name = f'{ticker}--{timespan}--{multiplier}--{"adjusted" if adjusted == "true" else "raw"}.csv'
    file_path = os.path.join(destination_dir_path, _out_file_name)

    last_recorded_datetime = None
    from_date = None
    if os.path.exists(file_path):
        last_recorded_datetime = get_most_recent_record_datetime(file_path)
        if last_recorded_datetime is not None:
            from_date = str(last_recorded_datetime.date())

    if from_date is None:
        from_date = query_polygon_first_date(ticker=ticker, adjusted=adjusted)
        if from_date is None:
            logging.error(f'Error getting first polygon date {ticker}')
            return

    df = get_aggregate_bars(ticker=ticker, from_date=from_date, to_date=UNIVERSAL_END_DATE,
                            multiplier=multiplier, adjusted=adjusted, timespan=timespan)

    if df is None:
        logging.error(f'Processed ticker {ticker}. Unsuccessful.')
        return
    logging.debug(f'Processed ticker {ticker}. Retrieved {len(df)} records from {from_date}')

    if last_recorded_datetime is not None and len(df) > 0:
        pre_len = len(df)
        new_records_indexes = df['datetime'] > last_recorded_datetime
        df = df.loc[new_records_indexes].copy()
        logging.debug(f'Removed {pre_len - len(df)} records since {from_date}')

    df.to_csv(file_path, index=False, mode='a', header=not os.path.exists(file_path))


def test():
    ticker = 'UPS'
    timespan = 'day'
    multiplier = 1
    from_date = '2019-12-28'
    to_date = '2021-08-05'
    adjusted = 'true'
    z = _get_bars(ticker=ticker, from_date=from_date, to_date=to_date, timespan=timespan, multiplier=multiplier,
                  adjusted=adjusted)

    for x in z:
        print(x)
    exit()


def main():
    key, destination_path, log_dir_path = get_config_elements()
    global API_KEY
    API_KEY = key
    log_file = os.path.join(log_dir_path, rf'outer-passage--{get_file_timestamp()}.log')
    logging.basicConfig(filename=log_file,
                        level=logging.DEBUG,
                        format='%(asctime)s\t%(levelname)s\t%(message)s')
    # test()

    logging.info('Connecting to client...')
    client = RESTClient(key)
    logging.info('Executing...')
    tickers = get_all_tickers(client=client,ticker_only=True)
    tickers = sorted(tickers,key=len)
    print(tickers[:10])
    exit()
    random.shuffle(tickers)
    logging.debug(f'Identified {len(tickers)} tickers.')
    ticker_count = 0

    for ticker in tickers:
        ticker_count += 1
        logging.info(f'{ticker_count}:{len(tickers)}\t{ticker}')
        for timespan, multiplier in period_list:
            for is_adjusted in adjusted_values:
                process_ticker(ticker=ticker, timespan=timespan, multiplier=multiplier, adjusted=is_adjusted,
                               destination_dir_path=destination_path)


if __name__ == "__main__":
    main()
