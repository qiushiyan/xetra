from xetra_jobs.s3.base_bucket import BaseBucketConnector
from xetra_jobs.common.utils import list_dates
import pandas as pd
from io import StringIO


class SourceBucketConnector(BaseBucketConnector):
    """
    interface to source bucket: deutsche-boerse-xetra-pds
    """

    def __init__(self, access_key, secret_access_key, endpoint_url, bucket_name):
        super().__init__(access_key, secret_access_key, endpoint_url, bucket_name)

    def list_keys_by_date_prefix(self, date_prefix):
        """
        lists objects filter by a prefix (date-like str)

        :param date_prefix: prefix to filter with, e.g. '2021-09-17'

        returns:
            a list of object keys that match the date prefix
        """
        keys = [obj.key for obj in self._bucket.objects.filter(
            Prefix=date_prefix)]
        return keys

    def read_object(self, key, columns=None, decoding="utf-8"):
        """
        read in an s3 object csv file as dataframe

        :param key: s3 object key
        :param columns: columns to select, passed to pd.read_csv(usecols)
        :param decoding: decoding codes, default to utf-8

        returns:
            a pandas dataframe with specified columns
        """
        self._logger.info(
            f'Reading file {self.endpoint_url}/{self._bucket.name}/{key}')
        columns = self.config["source"]["src_columns"] if not columns else columns
        try:
            csv_obj = self._bucket.Object(key=key)\
                .get()\
                .get("Body")\
                .read()\
                .decode(decoding)
            rows = len(csv_obj.strip().split("\n"))
            if rows >= 1:
                data = StringIO(csv_obj)
                if columns != "all":
                    df = pd.read_csv(data, usecols=columns)
                else:
                    df = pd.read_csv(data)
                return df
            else:
                return None
        except self.session.client("s3").exceptions.NoSuchKey:
            return None

    def read_objects(self, input_date: str, date_format: str, columns=None):
        """
        get all day's objects into a dataframe
        start from a date

        :param input_date: start date
        :param date_format: date format codes
        :param columns: columns to select, passed to pd.read_csv(usecols)

        returns:
            a dataframe concatting all day' objects
        """
        columns = self.config["source"]["src_columns"] if not columns else columns
        all_keys = []

        dates = list_dates(input_date, date_format, single_day=True)
        for date in dates:
            for key in self.list_keys_by_date_prefix(date):
                all_keys.append(key)
        df = pd.concat([self.read_object(key, columns)
                        for key in all_keys], ignore_index=True)
        return df
