from tests.s3.TestBaseBucketConnector import TestBaseBucketConnector
from xetra_jobs.common.exceptions import WrongFileFormatException
from xetra_jobs.common.constants import MetaFileConfig, S3TargetConfig
from datetime import datetime, timedelta
import pandas as pd
from io import StringIO, BytesIO
import unittest


class TestTargetBucketConnector(TestBaseBucketConnector):

    """
    test target bucket connector methdos
    """

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
        result = self.trg_bucket_connector.write_s3(df_expected, key, format)
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
        df_expected = pd.DataFrame(
            [['A', 'B'], ['C', 'D']], columns=['col1', 'col2'])
        key = 'test.parquet'
        file_format = 'parquet'
        # method execution
        self.trg_bucket_connector.write_s3(df_expected, key, file_format)

        data = self.bucket.Object(key=key).get().get(
            'Body').read()
        out_buffer = BytesIO(data)
        df_result = pd.read_parquet(out_buffer)
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
                self.trg_bucket_connector.write_s3(df, key, file_format)
            # log test after method execution
            self.assertIn(log_expected, log.output[0])

    def test_list_existing_target_dates(self):
        """
        test list_existing_target_dates returns correct date list
        """

        prefix = S3TargetConfig.PREFIX.value
        date1 = datetime.today().strftime("%Y-%m-%d")
        date2 = (datetime.today() - timedelta(1)).strftime("%Y-%m-%d")
        csv_content = """col1,col2
        valA,valB
        """
        key1 = f"{prefix}/{date1}.csv"
        key2 = f"{prefix}/{date2}.csv"
        self.bucket.put_object(Body=csv_content, Key=key1)
        self.bucket.put_object(Body=csv_content, Key=key2)
        # method execution
        dates_result = self.trg_bucket_connector.list_existing_target_dates()
        self.assertEqual(set([date1, date2]), set(dates_result))

    def test_read_meta_file(self):
        """
        test read_meta_file works when there is one
        """

        meta_key = MetaFileConfig.META_KEY.value
        date_col = MetaFileConfig.META_DATE_COL.value
        test_date = datetime.now().strftime(MetaFileConfig.META_DATE_FORMAT.value)
        timestamp_col = MetaFileConfig.META_TIMESTAMP_COL.value
        test_timestamp = datetime.now().strftime(
            MetaFileConfig.META_TIMESTAMP_FORMAT.value)
        csv_content = f"""{date_col}, {timestamp_col}
        {test_date}, {test_timestamp}
        """
        data = StringIO(csv_content)
        df_expcted = pd.read_csv(data)
        self.bucket.put_object(Body=csv_content, Key=meta_key)
        # method execution
        df_result = self.trg_bucket_connector.read_meta_file()
        self.assertTrue(df_result.equals(df_expcted))

    def test_read_meta_file_none(self):
        """
        test read_meta_file returns empty dataframe with correct columns
        when meta file does not exist yet in s3
        """

        meta_key = MetaFileConfig.META_KEY.value
        date_col = MetaFileConfig.META_DATE_COL.value
        timestamp_col = MetaFileConfig.META_TIMESTAMP_COL.value
        # delete any existing meta file
        self.bucket.Object(key=meta_key).delete()

        df_expcted = pd.DataFrame(columns=[date_col, timestamp_col])
        # method execution
        df_result = self.trg_bucket_connector.read_meta_file()
        self.assertTrue(df_result.equals(df_expcted))


if __name__ == '__main__':
    unittest.main()
