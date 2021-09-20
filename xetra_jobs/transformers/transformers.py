
from xetra_jobs.meta.meta_file import MetaFile
from xetra_jobs.s3.target_bucket import TargetBucketConnector
from xetra_jobs.s3.source_bucket import SourceBucketConnector
from xetra_jobs.transformers.config import ETLSourceConfig, ETLTargetConfig
import logging
from pandas import pd
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

    def extract(self):
        """
        read the source data and concatenates them into one pandas dataframe

        :returns:
          a pandas data frame containing data of the specified data
        """
        MetaFile.date_in_meta_file
        self._logger.info('extracting xetra source data')
        df = self.src_bucekt.read_objects(
            input_date=self.input_date, columns=self.src_args.src_columns)
        self._logger.info('extracted source data')
        return df

    def transform(self, df: pd.DataFrame):
        """
        apply transformations to extracted df

        :param df: result dataframe from extract()

        returns:
            df: the transformed dataframe
        """
        # return when data frame is emoty
        if df.empty:
            self._logger.info(
                'empty data frame, no transformation applied')
            return df
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
            .sort_values(by=[self.trg_args.src_col_date])\
            .groupby([self.trg_args.src_col_isin])[self.trg_args.trg_col_op_price]\
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
        return

    def load(self, df: pd.DataFrame):
        """
        save transformed dataframe to target bucket

        :param df: Pandas DataFrame as Input
        """
        target_key = (
            f'{self.trg_args.trg_key}'
            f'{datetime.today().strftime(self.trg_args.trg_key_date_format)}.'
            f'{self.trg_args.trg_format}'
        )
        self.trg_bucket.write_s3(df, target_key, self.trg_args.trg_format)
        self._logger.info(
            f'Saved transformed xetra data into target bucket ${target_key}')
        # Updating meta file
        MetaFile.update_meta_file(
            self.input_date, self.trg_bucket)
        self._logger.info('updated meta file')
        return True
