"""
Methods for processing meta file
"""
import collections
from datetime import datetime, timedelta

import pandas as pd

from xetra_jobs.common.s3 import S3BucketConnector
from xetra_jobs.common.constants import MetaFileFormat
from xetra_jobs.common.exceptions import WrongMetaFileException


class MetaFile():
    """
    class for working with the meta file
    """

    @staticmethod
    def create_meta_file(meta_key, bucket: S3BucketConnector):
        """
        create a meta file if not exists for existing reports
        """
        pass

    @staticmethod
    def update_meta_file(extract_date_list: list, meta_key: str, s3_bucket_meta: S3BucketConnector):
        """
        Updating the meta file with the processed Xetra dates and todays date as processed date

        :param: extract_date_list -> a list of dates that are extracted from the source
        :param: meta_key -> key of the meta file on the S3 bucket
        :param: s3_bucket_meta -> S3BucketConnector for the bucket with the meta file
        """
        # Creating an empty DataFrame using the meta file column names
        df_new = pd.DataFrame(columns=[
            MetaFileFormat.META_SOURCE_DATE_COL.value,
            MetaFileFormat.META_PROCESS_COL.value])
        # Filling the date column with extract_date_list
        df_new[MetaFileFormat.META_SOURCE_DATE_COL.value] = extract_date_list
        # Filling the processed column
        df_new[MetaFileFormat.META_PROCESS_COL.value] = \
            datetime.today().strftime(MetaFileFormat.META_PROCESS_DATE_FORMAT.value)
        try:
            # If meta file exists -> union DataFrame of old and new meta data is created
            df_old = s3_bucket_meta.read_csv_to_df(meta_key)
            if collections.Counter(df_old.columns) != collections.Counter(df_new.columns):
                raise WrongMetaFileException
            df_all = pd.concat([df_old, df_new])
        except s3_bucket_meta.session.client('s3').exceptions.NoSuchKey:
            # No meta file exists -> only the new data is used
            df_all = df_new
        # Writing to S3
        s3_bucket_meta.write_df_to_s3(
            df_all, meta_key, MetaFileFormat.META_FILE_FORMAT.value)
        return True

    @staticmethod
    def date_in_meta_file(date: str, meta_key: str, bucket: S3BucketConnector):
        """
        if a date has been stored in the metafile
        """
        meta_df = bucket.read_object(meta_key, columns=[
                                     MetaFileFormat.META_SOURCE_DATE_COL.value, MetaFileFormat.META_PROCESS_COL.value])
