from base64 import b64decode
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
import os

from .slack import SlackClient

import snowflake.connector
from snowflake.connector import DictCursor
from snowflake.connector import SnowflakeConnection

import os
from dotenv import load_dotenv
# import pandas
load_dotenv()
ACCOUNT = os.environ.get('GROPOD_ALERTS_SNOWFLAKE_ACCOUNT', None)
REGION = os.environ.get('GROPOD_ALERTS_SNOWFLAKE_REGION', None)
USER = os.environ.get('GROPOD_ALERTS_SNOWFLAKE_USER', None)
ROLE = os.environ.get('GROPOD_ALERTS_SNOWFLAKE_ROLE', None)
PRIVATE_KEY_BASE64 = os.environ.get('GROPOD_ALERTS_SNOWFLAKE_PRIVATE_KEY', None)
WH = os.environ.get('GROPOD_ALERTS_SNOWFLAKE_WH', None)

class SnowflakeClient():
    connection: SnowflakeConnection
    slackClient = SlackClient()

    def __init__(self):
        try:
            self.connection = snowflake.connector.connect(
                account=f"{ACCOUNT}.{REGION}",
                user=USER,
                private_key=self.__get_private_key_bytes(),
                role=ROLE,
                warehouse=WH
            )

        except Exception as e:
            print(e)
    
    def close(self):
        self.connection.close()
    
    def process_query(self, query, record_processor):
        cursor = self.connection.cursor(DictCursor)

        try:
            cursor.execute(query)

            for record in cursor:
                record_processor(record, self.slackClient)

        except Exception as e:
            print(e)

        finally:
            cursor.close()
    def __get_private_key_bytes(self):
        private_key = serialization.load_pem_private_key(
            data=b64decode(PRIVATE_KEY_BASE64),
            password=None,
            backend=default_backend()
        )

        return private_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.TraditionalOpenSSL,
            encryption_algorithm=serialization.NoEncryption(),
        )
    
