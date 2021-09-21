# xetra: a trading data ETL pipline

This repo contains example ETL jobs for processing daily trading data from the Deutsche Börse Group'

## Configuration

`configs/config.yaml` stores configurations for the ETL job

```yaml
# configuration specific to creating s3 connections
s3:
  src_endpoint_url: "https://s3.amazonaws.com"
  src_bucket: "deutsche-boerse-xetra-pds"
  trg_endpoint_url: # traget bucket url
  trg_bucket: # target bucket name
  access_key_name: # name of the environment variable of aws access key
  secret_access_key_name: # name of the environment variable of aws secret access key

# configuration specific to the source
source:
  src_input_date: # date to extract source data
  src_input_date_format: # input date format, e.g, %Y-%m-%d
  src_columns: [
      "ISIN",
      "Mnemonic",
      "Date",
      "Time",
      "StartPrice",
      "EndPrice",
      "MinPrice",
      "MaxPrice",
      "TradedVolume",
    ] # columns to extract from source data
  src_col_date: "Date"
  src_col_isin: "ISIN"
  src_col_time: "Time"
  src_col_min_price: "MinPrice"
  src_col_start_price: "StartPrice"
  src_col_max_price: "MaxPrice"
  src_col_traded_vol: "TradedVolume"

# configuration specific to the target
target:
  trg_prefix: "daily/" # prefix of saved data in target bucket
  trg_key_date_format: "%Y%m%d"
  trg_format: "parquet" # supports parquet or csv
  trg_col_isin: "isin"
  trg_col_date: "date"
  trg_col_op_price: "opening_price"
  trg_col_clos_price: "closing_price"
  trg_col_min_price: "min_price"
  trg_col_max_price: "max_price"
  trg_col_dail_trad_vol: "traded_volume"
  trg_col_ch_prev_clos: "pct"

# logging configuration
logging: ...
```
