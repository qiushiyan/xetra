"""
Methods for processing meta file
"""
import collections
from datetime import datetime
from io import StringIO
import pandas as pd
from xetra_jobs.s3.target_bucket import TargetBucketConnector
from xetra_jobs.common.constants import MetaFileConfig
from xetra_jobs.common.exceptions import WrongMetaFileException


class MetaFile():
    """
    class for working with the meta file
    """
    meta_key = MetaFileConfig.META_KEY.value

    @staticmethod
    def create_meta_file(bucket_connector: TargetBucketConnector):
        """
        create/update a meta file with all existing dates whose corresponding file has been uploaded to s3,
        set processing time to now

        :param bucket_connector:  a TargetBucketConnector instance to which the meta file will be uploaded to
        """
        # returns a list a processed dates
        existing_dates = bucket_connector.list_existing_dates()
        # create df for meta file
        df = pd.DataFrame(columns=[
            MetaFileConfig.META_DATE_COL.value,
            MetaFileConfig.META_TIMESTAMP_COL.value])
        df[MetaFileConfig.META_DATE_COL.value] = existing_dates
        df[MetaFileConfig.META_TIMESTAMP_COL.value] = \
            datetime.today().strftime(MetaFileConfig.META_TIMESTAMP_FORMAT.value)
        out_buffer = StringIO()
        df.to_csv(out_buffer, index=False)
        bucket_connector._put_object(out_buffer, key=MetaFile.meta_key)
        return True

    @staticmethod
    def update_meta_file(input_date: str,  bucket_connector: TargetBucketConnector):
        """
        update the meta file with the processed date and datetime.now() as processing time

        :param input_date: the processed date
        :param bucket -> a TargetBucketConnector instance in which the meta file will be updated
        """
        # Creating an empty DataFrame using the meta file column names
        df_new = pd.DataFrame(columns=[
            MetaFileConfig.META_DATE_COL.value,
            MetaFileConfig.META_TIMESTAMP_COL.value])
        # Filling the date column with extract_date_list
        df_new[MetaFileConfig.META_DATE_COL.value] = [input_date]
        # Filling the processed column
        df_new[MetaFileConfig.META_TIMESTAMP_COL.value] = [datetime.today().strftime(
            MetaFileConfig.META_TIMESTAMP_FORMAT.value)]
        try:
            # If meta file exists -> union DataFrame of old and new meta data is created
            df_old = bucket_connector.read_meta_file()
            if collections.Counter(df_old.columns) != collections.Counter(df_new.columns):
                raise WrongMetaFileException
            df_all = pd.concat([df_old, df_new])
        except bucket_connector.session.client('s3').exceptions.NoSuchKey:
            # No meta file exists -> only the new data is used
            df_all = df_new
        # Writing to S3
        bucket_connector.write_s3(
            df_all, key=MetaFileConfig.META_KEY.value, file_format=MetaFileConfig.META_FILE_FORMAT.value)
        return True

    @staticmethod
    def date_in_meta_file(date: str, bucket_connector: TargetBucketConnector):
        """
        if a date has been stored in the metafile
        """
        meta_df = bucket_connector.read_object(MetaFile.meta_key, columns=[
            MetaFileConfig.META_DATE_COL.value])
        return date in meta_df[MetaFileConfig.META_DATE_COL.value]
