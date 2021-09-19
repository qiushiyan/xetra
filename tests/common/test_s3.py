""" Test S3BucketConnection Methods"""

from io import StringIO, BytesIO
import os
import unittest
import boto3
from moto import mock_s3
from xetra_jobs.common.s3 import S3BucketConnector
from xetra_jobs.common.exceptions import WrongFileFormatException
import pandas as pd


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
        self.s3_client = boto3.resource(
            service_name='s3', endpoint_url=self.endpoint_url)
        self.s3_client.create_bucket(Bucket=self.bucket_name)
        self.bucket = self.s3_client.Bucket(self.bucket_name)
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

    def test_read_object(self):
        """
        test reading s3 objects as dataframe works
        """

        key = "test_csv"
        csv_content = """col1, col2
        valA, ValB
        """
        # the expected dataframe
        result_expected = pd.read_csv(StringIO(csv_content), usecols=["col1"])
        # mock upload csv to s3
        self.bucket.put_object(Body=csv_content, Key=key)
        result = self.bucket_connector.read_object(key, columns=["col1"])
        self.assertTrue(result.equals(result_expected))

    def test_write_s3_empty(self):
        """
        test write_s3 return None and log correctly if the dataframe is empty
        """
        log_expected = 'The dataframe is empty! No file will be written!'
        # Test init
        df_empty = pd.DataFrame()
        key = 'key.csv'
        format = 'csv'
        # Method execution
        with self.assertLogs() as log:
            result = self.bucket_connector.write_s3(df_empty, key, format)
            # Log test after method execution
            self.assertIn(log_expected, log.output[0])
        # Test after method execution
        self.assertEqual(None, result)

    def test_write_s3_csv(self):
        """
        test write_s3 works for csv
        """
        # Expected results
        return_expected = True
        df_expected = pd.DataFrame(
            [['A', 'B'], ['C', 'D']], columns=['col1', 'col2'])
        key = 'test.csv'
        format = 'csv'
        # Method execution
        result = self.bucket_connector.write_s3(df_expected, key, format)
        # Test after method execution
        data = self.bucket.Object(key=key).get().get(
            'Body').read().decode('utf-8')
        out_buffer = StringIO(data)
        df_result = pd.read_csv(out_buffer)
        self.assertEqual(return_expected, result)
        self.assertTrue(df_result.equals(df_expected))

    def test_write_s3_parquet(self):
        """
        test write_s3 works for parquet
        """
        # Expected results
        return_expected = True
        df_expected = pd.DataFrame(
            [['A', 'B'], ['C', 'D']], columns=['col1', 'col2'])
        key = 'test.parquet'
        format = 'parquet'
        # Method execution
        result = self.bucket_connector.write_s3(df_expected, key, format)
        # Test after method execution
        data = self.bucket.Object(key=key).get().get(
            'Body').read()
        out_buffer = BytesIO(data)
        df_result = pd.read_parquet(out_buffer)
        self.assertEqual(return_expected, result)
        self.assertTrue(df_result.equals(df_expected))

    def test_write_s3_wrong_format(self):
        """
        test write_s3 generates expected error and logs when given unsupported file formats
        """
        df = pd.DataFrame([['A', 'B'], ['C', 'D']], columns=['col1', 'col2'])
        key = 'test.parquet'
        file_format = "wrong_format"
        log_expected = f'The file format {file_format} is not supported to be written to s3!'
        exception_expected = WrongFileFormatException
        # method execution
        with self.assertLogs() as log:
            with self.assertRaises(exception_expected):
                self.bucket_connector.write_s3(df, key, file_format)
            # log test after method execution
            self.assertIn(log_expected, log.output[0])


if __name__ == "__main__":
    unittest.main()
