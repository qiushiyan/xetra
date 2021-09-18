import os
import boto3
import logging


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

        self._s3 = self.session.resource(
            service_name='s3', endpoint_url=endpoint_url)
        self._bucket = self._s3.Bucket(bucket)

    def list_keys_by_date_prefix(self, date_prefix):
        """
        lists all objects keys given a prefix (date-like str)

        :param date_prefix: prefix to filter with

        returns:
            keys: a list of object keys that match the date prefix
        """
        keys = [obj.key for obj in self._bucket.objects.filter(
            Prefix=date_prefix)]
        return keys
