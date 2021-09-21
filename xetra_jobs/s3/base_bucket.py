import logging
import boto3
import os
import unittest


class BaseBucketConnector(unittest.TestCase):
    """
    base class for source and target bucket
    """

    def __init__(self, access_key_name: str, secret_access_key_name: str, endpoint_url: str, bucket_name: str):
        """
        Constructor for S3BucketConnector

        :param access_key: access key for accessing S3
        :param secret_key: secret key for accessing S3
        :param endpoint_url: s3 endpoint url
        :param bucket: s3 bucket name
        """
        self._logger = logging.getLogger(__name__)
        self.endpoint_url = endpoint_url
        self.session = boto3.Session(aws_access_key_id=os.environ[access_key_name],
                                     aws_secret_access_key=os.environ[secret_access_key_name])

        self._s3_client = self.session.resource(
            service_name='s3', endpoint_url=endpoint_url)
        self._bucket = self._s3_client.Bucket(bucket_name)
