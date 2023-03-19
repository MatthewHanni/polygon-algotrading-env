# Use this code snippet in your app.
# If you need more information about configurations
# or implementing the sample code, visit the AWS docs:
# https://aws.amazon.com/developer/language/python/

import boto3
import json
from botocore.exceptions import ClientError
import configparser
import logging
import datetime

CONFIG_PATH = 'configuration.conf'



def create_log_file(name):
    config = load_config()
    if not os.path.exists(out_dir):
        os.mkdir(out_dir)
    log_file_path = fr'{log_dir}\{name}--{datetime.datetime.now().strftime("%Y%m%d-%H%M%S")}.log'
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s\t%(levelname)s\t%(message)s',
        handlers=[
            logging.FileHandler(log_file_path),
            logging.StreamHandler()
        ]
    )
    logging.info('Start')


def load_config():
    config = configparser.ConfigParser()
    config.read(CONFIG_PATH)

    return config



def get_api_key():
    config = load_config()
    secret_name = "prod/hanni"
    region_name = "us-east-1"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        # For a list of exceptions thrown, see
        # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        raise e

    # Decrypts secret using the associated KMS key.
    secrets = json.loads(get_secret_value_response['SecretString'])
    secrets_key = config['polygon']['api_key_secrets_manager']
    return secrets[secrets_key]

