from tests.s3.test_base_bucket import TestBaseBucketConnector
from xetra_jobs.common.exceptions import WrongFileFormatException
from xetra_jobs.common.constants import MetaFileConfig
from datetime import datetime, timedelta
import pandas as pd
from io import StringIO, BytesIO
import unittest


class TestTargetBucketConnector(TestBaseBucketConnector):
    """
    test target bucket connector methdos
    """

    # for test date listing
    prefix = "daily/"
    # for test csv io
    test_csv_content = f"col1,col2\nvalA,valB"
    test_csv_key = "test.csv"
    test_csv_format = "csv"
    test_df = pd.DataFrame(
        [['A', 'B'], ['C', 'D']], columns=['col1', 'col2'])
    # for test parquet io
    test_parquet_key = 'test.parquet'
    test_parquet_format = 'parquet'

    # for testing working with meta file
    meta_key = MetaFileConfig.META_KEY.value
    date_col = MetaFileConfig.META_DATE_COL.value
    timestamp_col = MetaFileConfig.META_TIMESTAMP_COL.value
    date_format = MetaFileConfig.META_DATE_FORMAT.value
    test_date = datetime.today().strftime(MetaFileConfig.META_DATE_FORMAT.value)

    def test_read_object_csv(self):
        df_expected = self.test_df
        self.trg_bucket_connector.write_s3(
            df_expected, self.test_csv_key, self.test_csv_format)
        df_result = self.trg_bucket_connector.read_object(
            self.test_csv_key, self.test_csv_format)
        self.assertTrue(df_result.equals(df_expected))

    def test_read_object_parquet(self):
        df_expected = self.test_df
        self.trg_bucket_connector.write_s3(
            df_expected, self.test_parquet_key, self.test_parquet_format)
        df_result = self.trg_bucket_connector.read_object(
            self.test_parquet_key, self.test_parquet_format)
        self.assertTrue(df_result.equals(df_expected))

    def test_write_s3_csv(self):
        """
        test write_s3 works for csv
        """

        df_expected = self.test_df

        # method execution
        self.trg_bucket_connector.write_s3(
            df_expected, self.test_csv_key, self.test_csv_format)
        # Test after method execution
        data = self.bucket.Object(key=self.test_csv_key).get().get(
            'Body').read().decode('utf-8')
        out_buffer = StringIO(data)
        df_result = pd.read_csv(out_buffer)
        self.assertTrue(df_result.equals(df_expected))

    def test_write_s3_parquet(self):
        """
        test write_s3 works for parquet
        """

        df_expected = self.test_df
        # method execution
        self.trg_bucket_connector.write_s3(
            df_expected, self.test_parquet_key, self.test_parquet_format)
        data = self.bucket.Object(key=self.test_parquet_key).get().get(
            'Body').read()
        out_buffer = BytesIO(data)
        df_result = pd.read_parquet(out_buffer)
        self.assertTrue(df_result.equals(df_expected))

    def test_write_s3_wrong_format(self):
        """
        test write_s3 generates expected error and logs when given unsupported file formats
        """

        file_format = "wrong_format"
        log_expected = f'The file format {file_format} is not supported to be written to s3!'
        exception_expected = WrongFileFormatException
        # method execution
        with self.assertLogs() as log:
            with self.assertRaises(exception_expected):
                self.trg_bucket_connector.write_s3(
                    self.test_df, self.test_parquet_key, file_format)
            # log test after method execution
            self.assertIn(log_expected, log.output[0])

    def test_list_existing_dates(self):
        """
        test list_existing_dates returns correct date list
        """

        date1 = datetime.today().strftime(self.date_format)
        date2 = (datetime.today() - timedelta(1)).strftime(self.date_format)
        key1 = f"{self.prefix}/{date1}.csv"
        key2 = f"{self.prefix}/{date2}.csv"
        self.bucket.put_object(Body=self.test_csv_content, Key=key1)
        self.bucket.put_object(Body=self.test_csv_content, Key=key2)
        # method execution
        dates_result = self.trg_bucket_connector.list_existing_dates()
        self.assertEqual(set([date1, date2]), set(dates_result))

    def test_read_meta_file(self):
        """
        test read_meta_file works when there is one
        """

        test_timestamp = datetime.now().strftime(
            MetaFileConfig.META_TIMESTAMP_FORMAT.value)
        csv_content = f"{self.date_col},{self.timestamp_col}\n{self.test_date},{test_timestamp}"
        data = StringIO(csv_content)
        df_expcted = pd.read_csv(data)
        self.bucket.put_object(Body=csv_content, Key=self.meta_key)
        # method execution
        df_result = self.trg_bucket_connector.read_meta_file()
        self.assertTrue(df_result.equals(df_expcted))

    def test_read_meta_file_none(self):
        """
        test read_meta_file returns empty dataframe with correct columns
        when meta file does not exist yet in s3
        """

        # delete any existing meta file
        self.bucket.Object(key=self.meta_key).delete()

        df_expcted = pd.DataFrame(columns=[self.date_col, self.timestamp_col])
        # method execution
        df_result = self.trg_bucket_connector.read_meta_file()
        self.assertTrue(df_result.equals(df_expcted))


if __name__ == '__main__':
    unittest.main()
