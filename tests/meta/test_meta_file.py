"""
tests for the meta file
"""
from xetra_jobs.common.exceptions import WrongMetaFileException
from tests.s3.test_base_bucket import TestBaseBucketConnector
from xetra_jobs.meta.meta_file import MetaFile
from xetra_jobs.common.constants import MetaFileConfig
from datetime import datetime
import pandas as pd
import unittest


class TestMetaFile(TestBaseBucketConnector):

    meta_key = MetaFileConfig.META_KEY.value
    meta_date_col = MetaFileConfig.META_DATE_COL.value
    meta_timestamp_col = MetaFileConfig.META_TIMESTAMP_COL.value
    meta_date_format = MetaFileConfig.META_DATE_FORMAT.value
    meta_timestamp_format = MetaFileConfig.META_TIMESTAMP_FORMAT.value
    target_prefix = "daily/"
    test_date = datetime.today().strftime(MetaFileConfig.META_DATE_FORMAT.value)

    def test_create_meta_file(self):
        """
        test create_meta_file works
        """
        # create a processed date file
        key = f"{self.target_prefix}{self.test_date}.csv"
        df_report = pd.DataFrame(columns=[
            "ISIN",
            "Mnemonic",
            "Date",
            "Time",
            "StartPrice",
            "EndPrice",
            "MinPrice",
            "MaxPrice",
            "TradedVolume",
        ])
        self.bucket.put_object(Body=df_report.to_csv(), Key=key)
        # list existing date (should be today)
        existing_dates = self.trg_bucket_connector.list_existing_dates()
        df_expected = pd.DataFrame(columns=[
            self.meta_date_col,
            self.meta_timestamp_col])
        df_expected[self.meta_date_col] = existing_dates
        df_expected[self.meta_timestamp_col] = \
            datetime.today().strftime(self.meta_timestamp_format)
        # method execution
        MetaFile.create_meta_file(self.trg_bucket_connector)
        df_result = self.trg_bucket_connector.read_meta_file()
        self.assertTrue(df_result.equals(df_expected))

    def test_update_meta_file(self):
        """
        test update_meta_file works when it does not exist
        """

        df_expected = pd.DataFrame({self.meta_date_col: [
                                   self.test_date], self.meta_timestamp_col: [datetime.today().strftime(self.meta_timestamp_format)]})
        # method execution
        MetaFile.update_meta_file(self.test_date, self.trg_bucket_connector)
        df_result = self.trg_bucket_connector.read_meta_file()
        self.assertTrue(df_result.equals(df_expected))

    def test_update_meta_file_meta_file_exists(self):
        """
        test update_meta_file works when there is one meta file already
        """
        # Expected results
        date_old = '2021-09-13'
        date_new = '2021-09-14'
        dates_expected = [date_old, date_new]

        meta_content = f"{self.meta_date_col},{self.meta_timestamp_col}\n{date_old},{datetime.today().strftime(self.meta_timestamp_format)}"
        self.bucket.put_object(Body=meta_content, Key=self.meta_key)
        # method execution
        MetaFile.update_meta_file(date_new, self.trg_bucket_connector)
        # read meta file
        df_result = self.trg_bucket_connector.read_meta_file()
        dates_result = list(df_result[
            self.meta_date_col])
        self.assertEqual(dates_expected, dates_result)

    def test_update_meta_file_meta_file_wrong(self):
        """
        test update_meta_file throws error when there is a meta file with wrong columns
        """
        date_old = '2021-09-13'
        date_new = '2021-09-14'
        meta_content = (
            f'wrong_column,{self.meta_timestamp_col}\n'
            f'{date_old},' f'{datetime.today().strftime(self.meta_timestamp_format)}\n'
        )
        self.bucket.put_object(Body=meta_content, Key=self.meta_key)
        # method execution
        with self.assertRaises(WrongMetaFileException):
            MetaFile.update_meta_file(
                date_new, self.trg_bucket_connector)

    def test_date_in_meta_file(self):
        """
        test date_in_meta_file works
        """
        MetaFile.update_meta_file(self.test_date, self.trg_bucket_connector)
        result1 = MetaFile.date_in_meta_file(
            self.test_date, self.trg_bucket_connector)
        result2 = MetaFile.date_in_meta_file(
            "2021-01-01", self.trg_bucket_connector)
        self.assertTrue(result1)
        self.assertFalse(result2)


if __name__ == '__main__':
    unittest.main()
