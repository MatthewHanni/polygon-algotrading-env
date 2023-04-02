import boto3
import json
from botocore.exceptions import ClientError
import configparser
import logging
import string
import random
import datetime
import os
import logging

CONFIG_PATH = 'configuration.conf'


def get_file_timestamp():
    return datetime.datetime.now().strftime("%Y%m%d-%H%M%S")


def load_config():
    config = configparser.ConfigParser()
    config.read(CONFIG_PATH)
    return config


def get_nonce(length=5):
    '''
        Returns a random 5-digit string. Used to prevent (significantly reduce) naming collisions.

                Parameters:
                        length (int): Desired nonce length
                Returns:
                        result_str (str): randomized uppercase+digits nonce of the length specified in the arguments
        '''
    letters = string.ascii_uppercase + string.digits
    result_str = ''.join(random.choice(letters) for i in range(length))
    return result_str

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

