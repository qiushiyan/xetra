"""
File to store constants
"""
from enum import Enum


class S3FileFormats(Enum):
    """
    supported file formats for S3BucketConnector
    """
    CSV = 'csv'
    PARQUET = 'parquet'


class MetaFileFormat(Enum):
    """
    formation for MetaProcess class
    """
    META_DATE_FORMAT = '%Y-%m-%d'
    META_PROCESS_DATE_FORMAT = '%Y-%m-%d %H:%M:%S'
    META_SOURCE_DATE_COL = 'date'
    META_PROCESS_COL = 'processing_time'
    META_FILE_FORMAT = 'csv'
