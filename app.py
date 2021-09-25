from xetra_jobs.common.constants import MetaFileConfig
from xetra_jobs.transformers.transformers import ETL
from xetra_jobs.transformers.config import ETLSourceConfig, ETLTargetConfig
from xetra_jobs.s3.target_bucket import TargetBucketConnector
from xetra_jobs.s3.source_bucket import SourceBucketConnector
import yaml
from flask import Flask, Response
from dateutil.parser import parse
import logging
import logging.config

app = Flask(__name__)


@app.route('/daily', defaults={'input_date': None})
@app.route('/daily/<input_date>', methods=["GET"])
def get_daily(input_date):
    with open("configs/config.yaml") as f:
        config = yaml.safe_load(f)
    try:
        input_date = parse(input_date).strftime(
            config["source"]["src_input_date_format"])
        config["source"]["src_input_date"] = input_date
    except:
        pass
    log_config = config['logging']
    logging.config.dictConfig(log_config)
    logger = logging.getLogger(__name__)
    s3_config = config['s3']
    src_bucket = SourceBucketConnector(access_key_name=s3_config['access_key_name'],
                                       secret_access_key_name=s3_config['secret_access_key_name'],
                                       endpoint_url=s3_config['src_endpoint_url'],
                                       bucket_name=s3_config['src_bucket'])
    trg_bucekt = TargetBucketConnector(access_key_name=s3_config['access_key_name'],
                                       secret_access_key_name=s3_config['secret_access_key_name'],
                                       endpoint_url=s3_config['trg_endpoint_url'],
                                       bucket_name=s3_config['trg_bucket'])
    src_config = ETLSourceConfig(**config['source'])
    trg_config = ETLTargetConfig(**config['target'])
    logger.info(f'xetra job started for {src_config.src_col_date}')
    etl = ETL(src_bucket, trg_bucekt,
              MetaFileConfig.META_KEY.value, src_config, trg_config)
    df = etl.run()
    logger.info(f'xetra job finished for {src_config.src_col_date}')
    return Response(df.to_json(orient="records"), mimetype='application/json')
