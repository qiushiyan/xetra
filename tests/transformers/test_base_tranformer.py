from xetra_jobs.common.constants import MetaFileConfig
from xetra_jobs.s3.source_bucket import SourceBucketConnector
from xetra_jobs.s3.target_bucket import TargetBucketConnector
from xetra_jobs.transformers.config import ETLSourceConfig, ETLTargetConfig
from xetra_jobs.transformers.transformers import ETL
import boto3
from moto import mock_s3
import unittest
import os
import pandas as pd
import unittest
from datetime import datetime


@mock_s3
class TestBaseETL(unittest.TestCase):
    """
    base class for testing ETL methods,
    """

    def setUp(self):
        self.bucket_config = {
            "access_key_name": "AWS_ACCESS_KEY",
            "secret_access_key_name": "AWS_SECRET_ACCESS_KEY",
            "endpoint_url": "https://s3.us-east-1.amazonaws.com",
            "bucket_name": "test-bucket"
        }
        os.environ[self.bucket_config["access_key_name"]] = "accesskey"
        os.environ[self.bucket_config["secret_access_key_name"]
                   ] = "secretaccesskey"
        self.s3_client = boto3.resource(
            service_name='s3', endpoint_url=self.bucket_config["endpoint_url"])
        self.s3_client.create_bucket(Bucket=self.bucket_config["bucket_name"])
        self.bucket = self.s3_client.Bucket(self.bucket_config["bucket_name"])
        # create bucket connector instance
        self.src_bucket_connector = SourceBucketConnector(**self.bucket_config)
        self.trg_bucket_connector = TargetBucketConnector(**self.bucket_config)
        # create target and source configuration
        self.meta_key = MetaFileConfig.META_KEY.value
        config = {"source": {
            'src_input_date': '2021-04-17',
            "src_input_date_format": "%Y-%m-%d",
            'src_columns': ['ISIN', 'Mnemonic', 'Date', 'Time',
                            'StartPrice', 'EndPrice', 'MinPrice', 'MaxPrice', 'TradedVolume'],
            'src_col_date': 'Date',
            'src_col_isin': 'ISIN',
            'src_col_time': 'Time',
            'src_col_start_price': 'StartPrice',
            'src_col_min_price': 'MinPrice',
            'src_col_max_price': 'MaxPrice',
            'src_col_traded_vol': 'TradedVolume'
        }, "target": {
            "trg_prefix": "daily/",
            "trg_key_date_format": "%Y%m%d",
            'trg_format': 'parquet',
            'trg_col_isin': 'isin',
            'trg_col_date': 'date',
            'trg_col_op_price': 'opening_price',
            'trg_col_clos_price': 'closing_price',
            'trg_col_min_price': 'min_price',
            'trg_col_max_price': 'max_price',
            'trg_col_dail_trad_vol': 'daily_traded_volume',
            'trg_col_ch_prev_clos': 'pct',
        }}
        # create source and transformed data
        self.src_config = ETLSourceConfig(**config["source"])
        self.trg_config = ETLTargetConfig(**config["target"])
        self.input_date = self.src_config.src_input_date
        self.input_date_format = self.src_config.src_input_date_format
        self.trg_key = (f'{self.trg_config.trg_prefix}'
                        f'{datetime.strptime(self.input_date, self.input_date_format).strftime(self.trg_config.trg_key_date_format)}.'
                        f'{self.trg_config.trg_format}'
                        )
        self.df_src = pd.DataFrame([['AT0000A0E9W5', 'SANT', '2021-04-16',
                                     '15:00', 18.27, 21.19, 18.27, 21.34, 987],
                                    ['AT0000A0E9W5', 'SANT', '2021-04-17',
                                     '13:00', 20.21, 18.27, 18.21, 20.42, 633],
                                    ['AT0000A0E9W5', 'SANT', '2021-04-17',
                                     '14:00', 18.27, 21.19, 18.27, 21.34, 455],
                                    ], columns=['ISIN', 'Mnemonic', 'Date', 'Time', 'StartPrice',
                                                'EndPrice', 'MinPrice', 'MaxPrice', 'TradedVolume'])

        self.trg_bucket_connector.write_s3(self.df_src.loc[0:0],
                                           '2021-04-16/2021-04-16_BINS_XETR15.csv', 'csv')
        self.trg_bucket_connector.write_s3(self.df_src.loc[1:1],
                                           '2021-04-17/2021-04-17_BINS_XETR13.csv', 'csv')
        self.trg_bucket_connector.write_s3(self.df_src.loc[2:2],
                                           '2021-04-17/2021-04-17_BINS_XETR14.csv', 'csv')

        self.df_trg = pd.DataFrame([['AT0000A0E9W5', '2021-04-17', 20.21, 18.27, 18.21, 21.34, 1088, 10.62],
                                    ], columns=['isin', 'date', 'opening_price', 'closing_price',
                                                'min_price', 'max_price', 'daily_traded_volume', 'pct'])
        self.etl = ETL(self.src_bucket_connector, self.trg_bucket_connector,
                       self.meta_key, self.src_config, self.trg_config)
        self.df_empty = pd.DataFrame()
        # meta file attributes
        self.meta_date_col = MetaFileConfig.META_DATE_COL.value

    def tearDown(self):
        for key in self.bucket.objects.all():
            key.delete()
        self.bucket.delete()
