""" Test SourceBucketConnector Methods"""

from io import StringIO
import unittest
import pandas as pd
from tests.s3.test_base_bucket import TestBaseBucketConnector


class TestSourceBucketConnector(TestBaseBucketConnector):
    """
    test source bucket connector methods
    """

    test_csv_key = "test.csv"
    test_csv_content = "col1,col2\nvalA.valB"

    def test_list_keys_by_date_prefix(self):
        """
        Test keys listing works
        """

        # test init
        prefix1 = "2021-09-18"
        prefix2 = "2021-09-20"
        key1 = f"{prefix1}/test.csv"
        key2 = f"{prefix2}/test.csv"
        self.bucket.put_object(Body=self.test_csv_content, Key=key1)
        self.bucket.put_object(Body=self.test_csv_content, Key=key2)
        # method execution
        result = self.src_bucket_connector.list_keys_by_date_prefix(
            date_prefix=prefix1)
        # tests
        self.assertIn(key1, result)
        self.assertNotIn(key2, result)

    def test_list_keys_by_date_prefix_wrong_prefix(self):
        """
        Test with wrong date prefix
        """

        prefix = "prefix-that-does-not-exist"
        result = self.src_bucket_connector.list_keys_by_date_prefix(
            date_prefix=prefix)
        self.assertFalse(result)

    def test_read_object(self):
        """
        test reading s3 objects as dataframe works
        """

        # the expected dataframe
        result_expected = pd.read_csv(
            StringIO(self.test_csv_content), usecols=["col1"])
        # mock upload csv to s3
        self.bucket.put_object(
            Body=self.test_csv_content, Key=self.test_csv_key)
        result = self.src_bucket_connector.read_object(
            self.test_csv_key, columns=["col1"])
        self.assertTrue(result.equals(result_expected))

    def test_read_objects(self):
        """
        test reading all objects with specific date prefix and concat them into a dataframe works
        """

        date1 = "2021-09-17"
        date2 = "2021-09-16"
        key1 = f"{date1}.csv"
        key2 = f"{date2}.csv"
        csv_content1 = """col1,col2
        valA,valB
        """
        csv_content2 = """col1,col2
        valC,valD
        """
        # the expected dataframe
        df1 = pd.read_csv(StringIO(csv_content1))
        df2 = pd.read_csv(StringIO(csv_content2))
        csv_expected = (pd.concat([df2, df1])).to_csv(index=False)
        # mock upload csv to s3
        self.bucket.put_object(Body=csv_content1, Key=key1)
        self.bucket.put_object(Body=csv_content2, Key=key2)
        csv_result = (self.src_bucket_connector.read_objects(
            "2021-09-17", "all")).to_csv(index=False)
        self.assertEqual(csv_expected, csv_result)


if __name__ == "__main__":
    unittest.main()
