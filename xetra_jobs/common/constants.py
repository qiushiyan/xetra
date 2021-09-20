"""
File to store constants
"""
from enum import Enum


class S3TargetConfig(Enum):
    """
    configuration for target bucket
    """
    PREFIX = "daily/"


class S3SourceConfig(Enum):
    """
    configuration for source bucket
    """
    INPUT_DATE_FORMAT = "%Y-%m-%d"


class S3FileFormats(Enum):
    """
    supported file formats for S3BucketConnector
    """
    CSV = 'csv'
    PARQUET = 'parquet'


class MetaFileConfig(Enum):
    """
    formation for MetaFile class
    """
    META_DATE_FORMAT = '%Y-%m-%d'
    META_TIMESTAMP_FORMAT = '%Y-%m-%d %H:%M:%S'
    META_DATE_COL = 'date'
    META_TIMESTAMP_COL = 'processing_time'
    META_FILE_FORMAT = 'csv'
    META_KEY = "meta.csv"
