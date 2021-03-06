"""entry to run ETL jobs"""
import logging
import logging.config
import yaml
import argparse
from xetra_jobs.common.constants import MetaFileConfig
from xetra_jobs.transformers.transformers import ETL
from xetra_jobs.transformers.config import ETLSourceConfig, ETLTargetConfig
from xetra_jobs.s3.target_bucket import TargetBucketConnector
from xetra_jobs.s3.source_bucket import SourceBucketConnector


def main():
    """
    entry to run ETL jobs
    """

    parser = argparse.ArgumentParser(description='run xetra etl job')
    parser.add_argument(
        '--config', help='a yaml configuration file', default="configs/config.yaml")
    args = parser.parse_args()
    with open(args.config) as f:
        config = yaml.safe_load(f)

    # with open("configs/config.yaml") as f:
    #     config = yaml.safe_load(f)

    # configure logging
    log_config = config['logging']
    logging.config.dictConfig(log_config)
    logger = logging.getLogger(__name__)
    # reading s3 configuration
    s3_config = config['s3']
    # creating the S3BucketConnector class instances for source and target
    src_bucket = SourceBucketConnector(access_key_name=s3_config['access_key_name'],
                                       secret_access_key_name=s3_config['secret_access_key_name'],
                                       endpoint_url=s3_config['src_endpoint_url'],
                                       bucket_name=s3_config['src_bucket'])
    trg_bucekt = TargetBucketConnector(access_key_name=s3_config['access_key_name'],
                                       secret_access_key_name=s3_config['secret_access_key_name'],
                                       endpoint_url=s3_config['trg_endpoint_url'],
                                       bucket_name=s3_config['trg_bucket'])
    # reading source configuration
    src_config = ETLSourceConfig(**config['source'])
    # reading target configuration
    trg_config = ETLTargetConfig(**config['target'])
    # creating XetraETL class instance
    logger.info(f'xetra job started for {src_config.src_col_date}')
    etl = ETL(src_bucket, trg_bucekt,
              MetaFileConfig.META_KEY.value, src_config, trg_config)
    # running etl job for xetra report1
    df = etl.run()
    logger.info(f'xetra job finished for {src_config.src_col_date}')
    print(
        f"transformed dataframe saved to target bucket {s3_config['trg_bucket']}, example: ")
    print(df.head())
    return df


if __name__ == "__main__":
    main()
