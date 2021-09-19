import boto3
from moto import mock_s3
import unittest
import os
from xetra_jobs.s3.SourceBucketConnector import SourceBucketConnector
from xetra_jobs.s3.TargetBucketConnector import TargetBucketConnector


@mock_s3
class TestBaseBucketConnector(unittest.TestCase):
    """
    base class for testing source and target bucket connector
    """

    def setUp(self):
        config = {
            "access_key": "AWS_ACCESS_KEY",
            "secret_access_key": "AWS_SECRET_ACCESS_KEY",
            "endpoint_url": "https://s3.us-east-1.amazonaws.com",
            "bucket_name": "test-bucket"
        }
        os.environ[config["access_key"]] = "accesskey"
        os.environ[config["secret_access_key"]] = "secretaccesskey"
        self.s3_client = boto3.resource(
            service_name='s3', endpoint_url=config["endpoint_url"])
        self.s3_client.create_bucket(Bucket=config["bucket_name"])
        self.bucket = self.s3_client.Bucket(config["bucket_name"])
        # Creating a testing instance
        self.src_bucket_connector = SourceBucketConnector(**config)
        self.trg_bucket_connector = TargetBucketConnector(**config)

    def tearDown(self):
        for key in self.bucket.objects.all():
            key.delete()
        self.bucket.delete()


if __name__ == "__main__":
    unittest.main()
