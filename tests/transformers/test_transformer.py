from xetra_jobs.transformers.transformers import ETL
import pandas as pd
from tests.transformers.test_base_tranformer import TestBaseETL
import unittest
from unittest.mock import patch


class TestETL(TestBaseETL):
    """
    tests for ETL, extract(), transform() and load()
    """

    def test_extract_no_file(self):
        """
        test extract returns empty dataframe for non-existing date in source bucket
        """
        with patch.object(self.src_bucket_connector, "list_keys_by_date_prefix", return_value=[]):
            df, _ = self.etl.extract()
        self.assertTrue(df.empty)

    def test_extract(self):
        """
        test extract works for correct date
        """
        # method execution
        df_result, _ = self.etl.extract()
        self.assertTrue(df_result.equals(self.df_src))

    def test_transform_empty(self):
        """
        test transform returns an empty data frame when given an empty data frame
        """
        df_expected = pd.DataFrame()
        df_result, transformed = self.etl.transform(pd.DataFrame())
        self.assertTrue(df_result.equals(df_expected))
        self.assertTrue(transformed)

    def test_transform(self):
        """
        test transform works
        """
        df_extracted, _ = self.etl.extract()
        df_result, _ = self.etl.transform(df_extracted)
        self.assertTrue(df_result.equals(self.df_trg))

    def test_load(self):
        """
        test load saves dataframe and updates meta file
        """
        df_extracted, _ = self.etl.extract()
        df_transformed, _ = self.etl.transform(df_extracted)
        df_expected = self.etl.load(df_transformed)

        df_result = self.trg_bucket_connector.read_object(
            self.trg_key, self.target_config.trg_format)
        meta_file = self.trg_bucket_connector.read_meta_file()

        self.assertTrue(df_result.equals(df_expected))
        self.assertIn(self.input_date,
                      (meta_file[self.meta_date_col]).tolist())

    def test_run_existed(self):
        """
        test run works when the dataframe has been loaded
        """
        df_extracted, _ = self.etl.extract()
        df_transformed, _ = self.etl.transform(df_extracted)
        self.etl.load(df_transformed)

        df_result, transformed = self.etl.extract()
        _, loaded = self.etl.transform(
            df_result, transformed)

        self.assertTrue(transformed)
        self.assertTrue(loaded)
        self.assertTrue(df_result.equals(self.df_trg))

    def test_load(self):
        """
        test run works
        """
        df_extracted, transformed = self.etl.extract()
        df_transformed, loaded = self.etl.transform(df_extracted)
        df_result = self.etl.load(df_transformed)
        self.assertFalse(transformed)
        self.assertFalse(loaded)
        self.assertTrue(df_result.equals(self.df_trg))


if __name__ == '__main__':
    unittest.main()
