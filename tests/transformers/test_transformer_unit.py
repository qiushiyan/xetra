from tests.transformers.test_base_tranformer import TestBaseETL
import unittest
from unittest.mock import patch


class UnitTestETL(TestBaseETL):
    """
    unit tests for ETL, extract(), transform() and load()
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
        df_result, transformed = self.etl.extract()
        self.assertTrue(df_result.equals(self.df_src))
        self.assertFalse(transformed)

    def test_transform_transformed_empty(self):
        """
        test transform is skipped when receiving transformed=True or an empty dataframe
        """
        log_expected_empty = "empty dataframe, skip transformation"
        log_expected_transformed = 'transformed dataframe, skip transformation'

        with self.assertLogs() as log:
            df_result_empty, loaded = self.etl.transform(self.df_empty)
            self.assertTrue(df_result_empty.equals(self.df_empty))
            self.assertTrue(loaded)
            self.assertIn(log_expected_empty, log.output[0])

        with self.assertLogs() as log:
            df_result_transformed, loaded = self.etl.transform(
                self.df_trg, transformed=True)
            self.assertTrue(df_result_transformed.equals(self.df_trg))
            self.assertTrue(loaded)
            self.assertIn(log_expected_transformed, log.output[0])

    def test_load_loaded(self):
        """
        test load is skipped when receiving loaded=True
        """
        log_expected = "dataframe has been loaded or is empty, skip loading"
        with self.assertLogs() as log:
            df_result = self.etl.load(self.df_trg, loaded=True)
            self.assertTrue(df_result.equals(self.df_trg))
            self.assertIn(log_expected, log.output[0])

    def test_load(self):
        """
        test load works in saving dataframe
        """
        endpoint_url = self.bucket_config["endpoint_url"]
        bucket_name = self.bucket.name

        log_expected1 = f'saving transformed data into target bucket {self.trg_key}'
        log_expected2 = f"writing file to {endpoint_url}/{bucket_name}/{self.trg_key}"
        log_expected3 = f'saved transformed data into target bucket {self.trg_key}'
        log_expected4 = f'reading meta file at {endpoint_url}/{bucket_name}/{self.meta_key}'
        log_expected5 = f"writing file to {endpoint_url}/{bucket_name}/{self.meta_key}"
        log_expected6 = f'updated meta file'

        # test logging output
        with self.assertLogs() as log:
            df_result = self.etl.load(self.df_trg)
            # Log test after method execution
            self.assertIn(log_expected1, log.output[0])
            self.assertIn(log_expected2, log.output[1])
            self.assertIn(log_expected3, log.output[2])
            self.assertIn(log_expected4, log.output[3])
            self.assertIn(log_expected5, log.output[4])
            self.assertIn(log_expected6, log.output[5])
        # test returning df
        self.assertTrue(df_result.equals(self.df_trg))
        # test saving df
        df_saved = self.trg_bucket_connector.read_object(
            self.trg_key, self.trg_config.trg_format)
        self.assertTrue(df_saved.equals(self.df_trg))
        # test updating meta file
        meta_df = self.trg_bucket_connector.read_meta_file()
        self.assertIn(self.input_date, meta_df[self.meta_date_col].tolist())


if __name__ == '__main__':
    unittest.main()
