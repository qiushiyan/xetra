from xetra_jobs.meta.meta_file import MetaFile
from xetra_jobs.s3.target_bucket import TargetBucketConnector
from xetra_jobs.s3.source_bucket import SourceBucketConnector
from xetra_jobs.transformers.config import ETLSourceConfig, ETLTargetConfig
import logging
import pandas as pd
from datetime import datetime


class ETL():
    """
    main interface to extract, transform and load xetra data
    """

    def __init__(self, source_bucket: SourceBucketConnector,
                 target_bucket: TargetBucketConnector, meta_key: str,
                 source_args: ETLSourceConfig, target_args: ETLTargetConfig):
        """
        Constructor for XetraTransformer

        :param source_bucket: connection to source S3 bucket
        :param target_bucket: connection to target S3 bucket
        :param meta_key: key of the meta file
        :param source_args: dataclass with source configuration data
        :param target_args: dataclass with target configuration data
        """
        self._logger = logging.getLogger(__name__)
        self.src_bucekt = source_bucket
        self.trg_bucket = target_bucket
        self.meta_key = meta_key
        self.src_args = source_args
        self.trg_args = target_args
        # just for convenience, since we are going to use the input date multiple times
        self.input_date = self.src_args.src_input_date
        self.input_date_format = self.src_args.src_input_date_format

    def extract(self):
        """
        read the source data and concatenates them into one pandas dataframe

        returns:
            a tuple with two elements
                1. df: the extracted dataframe
                2. transformed: should the dataframe been transformed

        """
        self._logger.info('extracting xetra source data')
        # if the input date is recorded in meta file, skip extract and transform steps
        # read directly from target bucket
        if MetaFile.date_in_meta_file(self.input_date, self.trg_bucket):
            self._logger.info(
                'input date exists in meta file, reading from target bucket')
            key = (
                f'{self.trg_args.trg_prefix}'
                f'{datetime.strptime(self.input_date, self.input_date_format).strftime(self.trg_args.trg_key_date_format)}.'
                f'{self.trg_args.trg_format}'
            )
            df = self.trg_bucket.read_object(key, self.trg_args.trg_format)
            self._logger.info('read data from target bucket')
            return (df, True)
        else:
            self._logger.info(
                'input date does not exist in meta file, reading from source bucket')
            df = self.src_bucekt.read_objects(
                input_date=self.input_date, columns=self.src_args.src_columns)
            self._logger.info('extracted data from source bucket')
            return (df, False)

    def transform(self, df: pd.DataFrame, transformed=False):
        """
        apply transformations to extracted df

        :param df: result dataframe from extract()
        :param transformed: if the dataframe has been transformed

        returns:
            a tuple contaning two elements:
                df: the transformed dataframe
                loaded: should the dataframe be loaded
        """
        # apply no transformation when df is read from target bucket
        if transformed:
            self._logger.info(
                'transformed dataframe, skip transformation')
            return (df, True)
        # apply no transformation when df is empty
        if df.empty:
            self._logger.info(
                'empty dataframe, skip transformation')
            return (df, True)
        # start transformation
        # drop rows with missing values
        df.dropna(inplace=True)
        # compute opening and closing price
        df[self.trg_args.trg_col_op_price] = df\
            .sort_values(by=[self.src_args.src_col_time])\
            .groupby([
                self.src_args.src_col_isin,
                self.src_args.src_col_date
            ])[self.src_args.src_col_start_price]\
            .transform('first')
        df[self.trg_args.trg_col_clos_price] = df\
            .sort_values(by=[self.src_args.src_col_time])\
            .groupby([
                self.src_args.src_col_isin,
                self.src_args.src_col_date
            ])[self.src_args.src_col_start_price]\
            .transform('last')
        # rename columns
        df.rename(columns={
            self.src_args.src_col_isin: self.trg_args.trg_col_isin,
            self.src_args.src_col_date: self.trg_args.trg_col_date,
            self.src_args.src_col_min_price: self.trg_args.trg_col_min_price,
            self.src_args.src_col_max_price: self.trg_args.trg_col_max_price,
            self.src_args.src_col_traded_vol: self.trg_args.trg_col_dail_trad_vol
        }, inplace=True)
        # Aggregating per ISIN and day -> opening price, closing price,
        # minimum price, maximum price, traded volume
        df = df.groupby([
            self.trg_args.trg_col_isin,
            self.trg_args.trg_col_date], as_index=False)\
            .agg({
                self.trg_args.trg_col_op_price: 'min',
                self.trg_args.trg_col_clos_price: 'min',
                self.trg_args.trg_col_min_price: 'min',
                self.trg_args.trg_col_max_price: 'max',
                self.trg_args.trg_col_dail_trad_vol: 'sum'})
        # Change of current day's closing price compared to the
        # previous trading day's closing price in %
        df[self.trg_args.trg_col_ch_prev_clos] = df\
            .sort_values(by=[self.trg_args.trg_col_date])\
            .groupby([self.trg_args.trg_col_isin])[self.trg_args.trg_col_op_price]\
            .shift(1)
        df[self.trg_args.trg_col_ch_prev_clos] = (
            df[self.trg_args.trg_col_op_price]
            - df[self.trg_args.trg_col_ch_prev_clos]
        ) / df[self.trg_args.trg_col_ch_prev_clos] * 100
        # select only data of input data, and rouding
        df = df[df[self.trg_args.trg_col_date] == self.input_date]\
            .reset_index(drop=True)\
            .round(decimals=2)
        self._logger.info(
            'applied transformations to source data')
        return (df, False)

    def load(self, df: pd.DataFrame, loaded=False):
        """
        save transformed dataframe to target bucket

        :param df: the dataframe to be saved
        :param loaded: if the dataframe has been loaded

        returns:
            the transformed dataframe
        """
        if loaded:
            self._logger.info(
                "data frame has been loaded or is empty, skip loading")
            return df
        else:
            target_key = (f'{self.trg_args.trg_prefix}'
                          f'{datetime.strptime(self.input_date, self.input_date_format).strftime(self.trg_args.trg_key_date_format)}.'
                          f'{self.trg_args.trg_format}'
                          )
            self._logger.info(
                f'saving transformed data into target bucket ${target_key}')
            self.trg_bucket.write_s3(df, target_key, self.trg_args.trg_format)
            self._logger.info(
                f'saved transformed data into target bucket ${target_key}')
            # Updating meta file
            MetaFile.update_meta_file(
                self.input_date, self.trg_bucket)
            self._logger.info('updated meta file')
            return df

    def run(self):
        """
        combine extract, transform, and load

        returns:
            the dataframe saved to target bucket
        """
        df, transformed = self.extract()
        df, loaded = self.transform(df, transformed)
        df = self.load(df, loaded)
        return df
