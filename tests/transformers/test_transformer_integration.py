from tests.transformers.test_base_tranformer import TestBaseETL
import unittest


class IntegrationTestETL(TestBaseETL):
    """
    integration unit tests for ETL, extract(), transform() and load()
    """

    def test_transform(self):
        """
        test transform works with extract
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

    def test_run(self):
        """
        test run works when receiving new date
        """
        df_extracted, transformed = self.etl.extract()
        df_transformed, loaded = self.etl.transform(df_extracted)
        df_result = self.etl.load(df_transformed)
        self.assertFalse(transformed)
        self.assertFalse(loaded)
        self.assertTrue(df_result.equals(self.df_trg))


if __name__ == '__main__':
    unittest.main()
