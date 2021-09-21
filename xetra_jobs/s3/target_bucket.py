from xetra_jobs.s3.base_bucket import BaseBucketConnector
from xetra_jobs.common.constants import MetaFileConfig,  S3FileFormats
from xetra_jobs.common.exceptions import WrongFileFormatException
from io import StringIO, BytesIO
import pandas as pd
import re


class TargetBucketConnector(BaseBucketConnector):
    """
    interface to target bucket: xetra-report
    """

    meta_key = MetaFileConfig.META_KEY.value
    meta_date_col = MetaFileConfig.META_DATE_COL.value
    meta_timestamp_col = MetaFileConfig.META_TIMESTAMP_COL.value

    csv_format = S3FileFormats.CSV.value
    parquet_format = S3FileFormats.PARQUET.value

    prefix = "daily/"

    def __init__(self, access_key, secret_access_key, endpoint_url, bucket_name):
        super().__init__(access_key, secret_access_key, endpoint_url, bucket_name)

    def read_meta_file(self, decoding="utf-8"):
        """
        retrieve meta file

        :param decoding: decoding codes

        returns:
            meta file in dataframe, returns empty dataframe if meta file does not exists
        """

        self._logger.info(
            f'reading meta file at {self.endpoint_url}/{self._bucket.name}/{self.meta_key}')
        try:
            csv_obj = self._bucket.Object(key=self.meta_key)\
                .get()\
                .get("Body")\
                .read()\
                .decode(decoding)
            data = StringIO(csv_obj)
            df = pd.read_csv(data)
        # if there is not meta file, return an empty data frame with specified columns
        except self.session.client("s3").exceptions.NoSuchKey:
            df = pd.DataFrame(columns=[
                self.meta_date_col,
                self.meta_timestamp_col])
        return df

    def read_object(self, key: str, file_format: str, decoding="utf-8"):
        """
        read in an s3 object as a pandas dataframe
        used as a caching layer when input date exists in meta file

        :param key: object key
        :param file_format: object file format, support csv or parquet
        :param decoding: file decoding for csv files

        returns:
            a dataframe
        """
        self._logger.info(
            f'reading file {self.endpoint_url}/{self._bucket.name}/{key}')
        if file_format == self.csv_format:
            csv_obj = self._bucket.Object(key=key)\
                .get()\
                .get("Body")\
                .read()\
                .decode(decoding)
            df = pd.read_csv(StringIO(csv_obj))
        if file_format == self.parquet_format:
            parquet_obj = self._bucket.Object(key=key)\
                .get()\
                .get("Body")\
                .read()
            df = pd.read_parquet(BytesIO(parquet_obj))
        return df

    def list_existing_dates(self):
        """
        list dates whose xetra data has been loaded to target bucket

        returns:
            a list of dates, without target prefix
        """

        existing_keys = [
            obj.key for obj in self._bucket.objects.filter(Prefix=self.prefix)]
        existing_dates = []
        for key in existing_keys:
            m = re.search(r'(\d+-\d+-\d+)', key)
            date = m.group(1)
            if date not in existing_dates:
                existing_dates.append(date)
        return existing_dates

    def write_s3(self, df: pd.DataFrame, key: str, file_format: str):
        """
        write a dataframe to S3
        supported formats: .csv, .parquet

        :param df: the dataframe that should be written
        :param key: key of the saved file in s3
        :param format: saving format
        """
        if df.empty:
            self._logger.info(
                'The dataframe is empty! No file will be written!')
            return None
        if file_format == self.csv_format:
            out_buffer = StringIO()
            df.to_csv(out_buffer, index=False)
            return self._put_object(out_buffer, key)
        if file_format == self.parquet_format:
            out_buffer = BytesIO()
            df.to_parquet(out_buffer, index=False)
            return self._put_object(out_buffer, key)
        self._logger.info(f'The file format {file_format} is not '
                          'supported to be written to s3!')
        raise WrongFileFormatException(file_format)

    def _put_object(self, out_buffer: StringIO or BytesIO, key: str):
        """
        Helper function for writing to s3

        :param out_buffer: output buffer, BytesIO for parquet, and StringIO for csv
        :param key: target key of the saved file
        """
        self._logger.info(
            f'Writing file to {self.endpoint_url}/{self._bucket.name}/{key}')
        self._bucket.put_object(Body=out_buffer.getvalue(), Key=key)
        return True
