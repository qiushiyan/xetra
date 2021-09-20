"""
tests for the meta file
"""
from xetra_jobs.meta.meta_file import MetaFile
from tests.s3.test_base_bucket import TestBaseBucketConnector
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


if __name__ == '__main__':
    unittest.main()
