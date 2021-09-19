""" Test S3BucketConnection Methods"""

import os
import unittest
import boto3
from moto import mock_s3
from xetra_jobs.common.s3 import S3BucketConnector


@mock_s3
class TestS3BucketConnector(unittest.TestCase):
    """
    Test S3BucketConnector methods
    """

    def setUp(self):
        self.access_key = "AWS_ACCESS_KEY"
        self.secret_access_key = "AWS_SECRET_ACCESS_KEY"
        self.endpoint_url = "https://s3.us-east-1.amazonaws.com"
        self.bucket_name = "test-bucket"
        os.environ[self.access_key] = "accesskey"
        os.environ[self.secret_access_key] = "secretaccesskey"
        self.s3 = boto3.resource(
            service_name='s3', endpoint_url=self.endpoint_url)
        self.s3.create_bucket(Bucket=self.bucket_name)
        self.bucket = self.s3.Bucket(self.bucket_name)
        # Creating a testing instance
        self.bucket_connector = S3BucketConnector(self.access_key,
                                                  self.secret_access_key,
                                                  self.endpoint_url,
                                                  self.bucket_name)

    def tearDown(self):
        for key in self.bucket.objects.all():
            key.delete()
        self.bucket.delete()

    def test_list_keys_by_date_prefix_ok(self):
        """
        Test keys listing works
        """
        # test init
        prefix1 = "2021-09-18"
        prefix2 = "2021-09-20"
        key1 = f"{prefix1}/test.csv"
        key2 = f"{prefix2}/test.csv"
        csv_content = """col1, col2
        valA, ValB
        """
        self.bucket.put_object(Body=csv_content, Key=key1)
        self.bucket.put_object(Body=csv_content, Key=key2)
        # method execution
        result = self.bucket_connector.list_keys_by_date_prefix(
            date_prefix=prefix1)
        # tests
        self.assertIn(key1, result)
        self.assertNotIn(key2, result)

    def test_list_keys_by_date_prefix_wrong_prefix(self):
        """
        Test with wrong date prefix
        """
        prefix = "prefix-that-does-not-exist"
        result = self.bucket_connector.list_keys_by_date_prefix(
            date_prefix=prefix)
        self.assertTrue(not result)


if __name__ == "__main__":
    unittest.main()
