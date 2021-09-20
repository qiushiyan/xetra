"""
tests for the meta file
"""
from tests.s3.test_base_bucket import TestBaseBucketConnector
from xetra_jobs.meta.meta_file import MetaFile
from xetra_jobs.common.constants import MetaFileConfig, S3TargetConfig
from datetime import datetime
import pandas as pd
import unittest


class TestMetaFile(TestBaseBucketConnector):

    def test_create_meta_file(self):
        """
        test create_meta_file works
        """
        # create a processed date file
        today = datetime.today().strftime(MetaFileConfig.META_DATE_FORMAT.value)
        key = f"{S3TargetConfig.PREFIX.value}/{today}.csv"
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
            MetaFileConfig.META_DATE_COL.value,
            MetaFileConfig.META_TIMESTAMP_COL.value])
        df_expected[MetaFileConfig.META_DATE_COL.value] = existing_dates
        df_expected[MetaFileConfig.META_TIMESTAMP_COL.value] = \
            datetime.today().strftime(MetaFileConfig.META_TIMESTAMP_FORMAT.value)
        # method execution
        MetaFile.create_meta_file(self.trg_bucket_connector)
        df_result = self.trg_bucket_connector.read_meta_file()
        self.assertTrue(df_result.equals(df_expected))

    def test_update_meta_file(self):
        """
        test update_meta_file works when it does not exist
        """

        input_date = datetime.today().strftime(MetaFileConfig.META_DATE_FORMAT.value)
        processing_time = datetime.today().strftime(
            MetaFileConfig.META_TIMESTAMP_FORMAT.value)
        df_expected = pd.DataFrame({MetaFileConfig.META_DATE_COL.value: [
                                   input_date], MetaFileConfig.META_TIMESTAMP_COL.value: [processing_time]})
        # method execution
        MetaFile.update_meta_file(input_date, self.trg_bucket_connector)
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
        processed_time_expected = [datetime.today().date()] * 2
        # Test init
        meta_key = MetaFileConfig.META_KEY.value
        meta_content = f"""
        {MetaFileConfig.META_DATE_COL.value}, {MetaFileConfig.META_TIMESTAMP_COL.value}
        {date_old}, {datetime.today().strftime(MetaFileConfig.META_DATE_COL.value)},
        """
        self.bucket.put_object(Body=meta_content, Key=meta_key)
        # Method execution
        MetaFile.update_meta_file(date_new, self.trg_bucket_connector)
        # Read meta file
        df_result = self.trg_bucket_connector.read_meta_file()
        dates_result = list(df_result[
            MetaFileConfig.META_DATE_COL.value])
        processed_time_result = list(df_result[
            MetaFileConfig.META_TIMESTAMP_COL.value])
        # Test after method execution
        self.assertEqual(dates_expected, dates_result)
        self.assertEqual(processed_time_expected, processed_time_result)


if __name__ == '__main__':
    unittest.main()
