import os
import boto3
import logging
from xetra_jobs.utils import list_dates
import pandas as pd
from io import StringIO, BytesIO
from xetra_jobs.common.constants import S3FileFormats
from xetra_jobs.common.exceptions import WrongFileFormatException


class S3BucketConnector:
    """
    interface to s3 bucket
    """

    def __init__(self, access_key: str, secret_access_key: str, endpoint_url: str, bucket: str):
        """Constructor for S3BucketConnector

        :param access_key: access key for accessing S3
        :param secret_key: secret key for accessing S3
        :param endpoint_url: s3 endpoint url
        :param bucket: s3 bucket name
        """
        self._logger = logging.getLogger(__name__)
        self.endpoint_url = endpoint_url
        self.session = boto3.Session(aws_access_key_id=os.environ[access_key],
                                     aws_secret_access_key=os.environ[secret_access_key])

        self._s3_client = self.session.resource(
            service_name='s3', endpoint_url=endpoint_url)
        self._bucket = self._s3_client.Bucket(bucket)

    def list_keys_by_date_prefix(self, date_prefix):
        """
        lists all objects keys given a prefix (date-like str)

        :param date_prefix: prefix to filter with

        returns:
            a list of object keys that match the date prefix
        """
        keys = [obj.key for obj in self._bucket.objects.filter(
            Prefix=date_prefix)]
        return keys

    def read_object(self, key, columns, decoding="utf-8"):
        """
        reads in an s3 object as dataframe
        """
        self._logger.info(
            f'Reading file {self.endpoint_url}/{self._bucket.name}/{key}')
        try:
            csv_obj = self._bucket.Object(key=key)\
                .get()\
                .get("Body")\
                .read()\
                .decode(decoding)
            rows = len(csv_obj.strip().split("\n"))
            if rows >= 1:
                data = StringIO(csv_obj)
                df = pd.read_csv(data, usecols=columns)
            else:
                df = pd.DataFrame(columns=columns)
        except:
            df = pd.DataFrame(columns=columns)
        return df

    def write_s3(self, df: pd.DataFrame, key: str, file_format: str):
        """
        writing a Pandas DataFrame to S3
        supported formats: .csv, .parquet

        :param df: the dataframe that should be written
        :param key: key of the saved file in s3
        :param format: saving format
        """
        if df.empty:
            self._logger.info(
                'The dataframe is empty! No file will be written!')
            return None
        if file_format == S3FileFormats.CSV.value:
            out_buffer = StringIO()
            df.to_csv(out_buffer, index=False)
            return self._put_object(out_buffer, key)
        if file_format == S3FileFormats.PARQUET.value:
            out_buffer = BytesIO()
            df.to_parquet(out_buffer, index=False)
            return self._put_object(out_buffer, key)
        self._logger.info(f'The file format {file_format} is not '
                          'supported to be written to s3!')
        raise WrongFileFormatException(file_format)

    def _put_object(self, out_buffer: StringIO or BytesIO, key: str):
        """
        Helper function for self.write_df_to_s3()

        :param out_buffer: output buffer, BytesIO for parquet, and StringIO for csv
        :param key: target key of the saved file
        """
        self._logger.info(
            f'Writing file to {self.endpoint_url}/{self._bucket.name}/{key}')
        self._bucket.put_object(Body=out_buffer.getvalue(), Key=key)
        return True
