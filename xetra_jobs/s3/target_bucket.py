from xetra_jobs.s3.base_bucket import BaseBucketConnector
from xetra_jobs.common.constants import MetaFileConfig, S3TargetConfig, S3FileFormats
from xetra_jobs.common.exceptions import WrongFileFormatException, WrongMetaFileException
from io import StringIO, BytesIO
import pandas as pd
import re


class TargetBucketConnector(BaseBucketConnector):
    """
    interface to target bucket: xetra-report
    """

    def __init__(self, access_key, secret_access_key, endpoint_url, bucket_name):
        super().__init__(access_key, secret_access_key, endpoint_url, bucket_name)

    def read_meta_file(self, decoding="utf-8"):
        """
        retrieve meta file

        :param decoding: decoding codes

        returns:
            meta file in dataframe, returns empty dataframe if meta file does not exists
        """

        meta_key = MetaFileConfig.META_KEY.value
        self._logger.info(
            f'reading meta file at {self.endpoint_url}/{self._bucket.name}/{meta_key}')
        try:
            csv_obj = self._bucket.Object(key=meta_key)\
                .get()\
                .get("Body")\
                .read()\
                .decode(decoding)
            data = StringIO(csv_obj)
            df = pd.read_csv(data)
        except self.session.client("s3").exceptions.NoSuchKey:
            df = pd.DataFrame(columns=[
                MetaFileConfig.META_DATE_COL.value,
                MetaFileConfig.META_TIMESTAMP_COL.value])
        return df

    def list_existing_dates(self):
        """
        list dates whose xetra data has been loaded to target bucket

        returns:
            a list of dates, without target prefix
        """

        trg_prefix = S3TargetConfig.PREFIX.value
        existing_keys = [
            obj.key for obj in self._bucket.objects.filter(Prefix=trg_prefix)]
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
        if file_format == S3FileFormats.CSV.value:
            out_buffer = StringIO()
            df.to_csv(out_buffer, index=False)
            return self._put_object(out_buffer, key)
        if file_format == S3FileFormats.PARQUET.value:
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
