# xetra: a trading data ETL pipline

This repo contains example ETL jobs for processing daily trading data from the Deutsche Börse Group.

## The Problem

The Deutsche Börse Public releases real-time trade data in their public s3 bucket `s3://deutsche-boerse-xetra-pds`, which is aggregated to one minute intervals from the Eurex and Xetra trading systems. It provides the initial price, lowest price, highest price, final price and volume for every minute of the trading day.

While minute-level data provides the highest resolution, analysts may be more intersted to get data on a daily basis. New features should be created such as opening price, closing price, minimum price, maximum price and growth rate comapred to previous workday.

Example source data

| ISIN         | Date       | Time     | StartPrice | MaxPrice | MinPrice | EndPrice | TradedVolume |
| :----------- | :--------- | :------- | ---------: | -------: | -------: | -------: | -----------: |
| AT0000A0E9W5 | 2021-09-16 | 07:00:00 |      22.50 |    22.52 |    22.48 |    22.48 |         3279 |
| DE000A0DJ6J9 | 2021-09-16 | 07:00:00 |      37.60 |    37.60 |    37.50 |    37.50 |         1281 |
| DE000A0D6554 | 2021-09-16 | 07:00:00 |      15.15 |    15.18 |    15.14 |    15.14 |         7066 |
| DE000A0D9PT0 | 2021-09-16 | 07:00:00 |     187.95 |   188.00 |   187.85 |   188.00 |          374 |
| DE000A0HN5C6 | 2021-09-16 | 07:00:00 |      53.02 |    53.02 |    52.98 |    53.02 |         9971 |
| DE000A0JL9W6 | 2021-09-16 | 07:00:00 |      53.75 |    53.75 |    53.45 |    53.70 |          676 |

Moreover, data of the same date in the source xetra bucket is separatd into multiple pieces (objects). For example, for trading data in 2017-08-01 we need to concat the following objects:

```bash
$ aws s3 ls deutsche-boerse-xetra-pds/2017-08-01/ --no-sign-request
2018-04-04 12:58:38        136 2017-08-01_BINS_XETR00.csv
2018-04-04 12:58:38        136 2017-08-01_BINS_XETR01.csv
2018-04-04 12:58:38        136 2017-08-01_BINS_XETR02.csv
2018-04-04 12:58:38        136 2017-08-01_BINS_XETR03.csv
2018-04-04 12:58:38        136 2017-08-01_BINS_XETR04.csv
2018-04-04 12:58:38        136 2017-08-01_BINS_XETR05.csv
2018-04-04 12:58:38        136 2017-08-01_BINS_XETR06.csv
2018-04-04 12:58:38    1016188 2017-08-01_BINS_XETR07.csv
2018-04-04 12:58:39     934078 2017-08-01_BINS_XETR08.csv
2018-04-04 12:58:38     863130 2017-08-01_BINS_XETR09.csv
2018-04-04 12:58:41     805186 2017-08-01_BINS_XETR10.csv
2018-04-04 12:58:38     749942 2017-08-01_BINS_XETR11.csv
2018-04-04 12:58:40     788177 2017-08-01_BINS_XETR12.csv
2018-04-04 12:58:40    1054569 2017-08-01_BINS_XETR13.csv
2018-04-04 12:58:39    1145654 2017-08-01_BINS_XETR14.csv
2018-04-04 12:58:41     712217 2017-08-01_BINS_XETR15.csv
2018-04-04 12:58:41        136 2017-08-01_BINS_XETR16.csv
2018-04-04 12:58:41        136 2017-08-01_BINS_XETR17.csv
2018-04-04 12:58:41        136 2017-08-01_BINS_XETR18.csv
2018-04-04 12:58:40        886 2017-08-01_BINS_XETR19.csv
2018-04-04 12:58:41        136 2017-08-01_BINS_XETR20.csv
2018-04-04 12:58:41        136 2017-08-01_BINS_XETR21.csv
2018-04-04 12:58:41        136 2017-08-01_BINS_XETR22.csv
2018-04-04 12:58:41        136 2017-08-01_BINS_XETR23.csv
```

Therefore, there is a need to extract daily data into a single table, apply transformations, and save the results in a target s3 bucket for future analysis.

## Usage

from the command line

```bash
python ./run.py --config configs/config.yaml
```

from api endpoints

```
flask run
```

and then navigate to http://127.0.0.1:5000/daily, add date as following subroute, e.g., http://127.0.0.1:5000/daily/20210917

Example processed data

| isin         | opening_price | closing_price | min_price | max_price | traded_volume |   pct |
| :----------- | ------------: | ------------: | --------: | --------: | ------------: | ----: |
| AT00000FACC2 |          9.65 |          9.38 |      9.35 |      9.65 |          1324 |  0.52 |
| AT0000606306 |         21.46 |         21.40 |     21.26 |     21.46 |          2486 |  0.37 |
| AT0000609607 |         17.24 |         17.18 |     17.06 |     17.24 |           256 |  0.47 |
| AT0000644505 |        103.20 |        101.80 |    101.20 |    103.20 |           429 | -1.71 |
| AT0000652011 |         35.17 |         34.99 |     34.96 |     35.49 |           180 |  0.20 |
| AT0000652250 |         20.60 |         20.75 |     20.50 |     20.75 |           600 |  0.49 |

A meta file is included as a caching layer and saved to s3 target bucket. Whenever the job for the requested day is finished, the meta file is also updated to track processed dates, for example:

```
# example content of the meta file
date,processing_time
2021-09-17,2021-09-20 20:37:33
2021-09-16,2021-09-20 20:42:53
2021-09-15,2021-09-21 14:16:31
```

If a job is started for a date that exists in the meta file `date` column, e.g. 2021-09-17, the processed data frame will be read directly from the target bucket instead of the source bucket.

## Configuration

`configs/config.yaml` stores global settings for the ETL job

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

## Logging

example log file for unprocessed date

```
2021-09-21 14:16:15,588 - __main__ - INFO - xetra job started for 2021-09-16
2021-09-21 14:16:15,588 - xetra_jobs.transformers.transformers - INFO - extracting xetra source data
2021-09-21 14:16:15,588 - xetra_jobs.s3.base_bucket - INFO - reading meta file at https://s3.amazonaws.com/xetra-report/meta.csv
2021-09-21 14:16:15,849 - xetra_jobs.transformers.transformers - INFO - input date does not exist in meta file, reading from source bucket
2021-09-21 14:16:16,996 - xetra_jobs.s3.base_bucket - INFO - reading file https://s3.amazonaws.com/deutsche-boerse-xetra-pds/2021-09-14/2021-09-14_BINS_XETR00.csv
2021-09-21 14:16:17,160 - xetra_jobs.s3.base_bucket - INFO - reading file https://s3.amazonaws.com/deutsche-boerse-xetra-pds/2021-09-14/2021-09-14_BINS_XETR01.csv
2021-09-21 14:16:17,331 - xetra_jobs.s3.base_bucket - INFO - reading file https://s3.amazonaws.com/deutsche-boerse-xetra-pds/2021-09-14/2021-09-14_BINS_XETR02.csv
2021-09-21 14:16:17,513 - xetra_jobs.s3.base_bucket - INFO - reading file https://s3.amazonaws.com/deutsche-boerse-xetra-pds/2021-09-14/2021-09-14_BINS_XETR03.csv
2021-09-21 14:16:17,677 - xetra_jobs.s3.base_bucket - INFO - reading file https://s3.amazonaws.com/deutsche-boerse-xetra-pds/2021-09-14/2021-09-14_BINS_XETR04.csv
2021-09-21 14:16:17,852 - xetra_jobs.s3.base_bucket - INFO - reading file https://s3.amazonaws.com/deutsche-boerse-xetra-pds/2021-09-14/2021-09-14_BINS_XETR05.csv
2021-09-21 14:16:18,027 - xetra_jobs.s3.base_bucket - INFO - reading file https://s3.amazonaws.com/deutsche-boerse-xetra-pds/2021-09-14/2021-09-14_BINS_XETR06.csv
2021-09-21 14:16:18,222 - xetra_jobs.s3.base_bucket - INFO - reading file https://s3.amazonaws.com/deutsche-boerse-xetra-pds/2021-09-14/2021-09-14_BINS_XETR07.csv
2021-09-21 14:16:19,420 - xetra_jobs.s3.base_bucket - INFO - reading file https://s3.amazonaws.com/deutsche-boerse-xetra-pds/2021-09-14/2021-09-14_BINS_XETR08.csv
2021-09-21 14:16:19,856 - xetra_jobs.s3.base_bucket - INFO - reading file https://s3.amazonaws.com/deutsche-boerse-xetra-pds/2021-09-14/2021-09-14_BINS_XETR09.csv
2021-09-21 14:16:20,177 - xetra_jobs.s3.base_bucket - INFO - reading file https://s3.amazonaws.com/deutsche-boerse-xetra-pds/2021-09-14/2021-09-14_BINS_XETR10.csv
2021-09-21 14:16:20,574 - xetra_jobs.s3.base_bucket - INFO - reading file https://s3.amazonaws.com/deutsche-boerse-xetra-pds/2021-09-14/2021-09-14_BINS_XETR11.csv
2021-09-21 14:16:20,921 - xetra_jobs.s3.base_bucket - INFO - reading file https://s3.amazonaws.com/deutsche-boerse-xetra-pds/2021-09-14/2021-09-14_BINS_XETR12.csv
2021-09-21 14:16:21,278 - xetra_jobs.s3.base_bucket - INFO - reading file https://s3.amazonaws.com/deutsche-boerse-xetra-pds/2021-09-14/2021-09-14_BINS_XETR13.csv
2021-09-21 14:16:21,790 - xetra_jobs.s3.base_bucket - INFO - reading file https://s3.amazonaws.com/deutsche-boerse-xetra-pds/2021-09-14/2021-09-14_BINS_XETR14.csv
2021-09-21 14:16:22,138 - xetra_jobs.s3.base_bucket - INFO - reading file https://s3.amazonaws.com/deutsche-boerse-xetra-pds/2021-09-14/2021-09-14_BINS_XETR15.csv
2021-09-21 14:16:22,489 - xetra_jobs.s3.base_bucket - INFO - reading file https://s3.amazonaws.com/deutsche-boerse-xetra-pds/2021-09-14/2021-09-14_BINS_XETR16.csv
2021-09-21 14:16:22,652 - xetra_jobs.s3.base_bucket - INFO - reading file https://s3.amazonaws.com/deutsche-boerse-xetra-pds/2021-09-14/2021-09-14_BINS_XETR17.csv
2021-09-21 14:16:22,828 - xetra_jobs.s3.base_bucket - INFO - reading file https://s3.amazonaws.com/deutsche-boerse-xetra-pds/2021-09-14/2021-09-14_BINS_XETR18.csv
2021-09-21 14:16:23,032 - xetra_jobs.s3.base_bucket - INFO - reading file https://s3.amazonaws.com/deutsche-boerse-xetra-pds/2021-09-14/2021-09-14_BINS_XETR19.csv
2021-09-21 14:16:23,199 - xetra_jobs.s3.base_bucket - INFO - reading file https://s3.amazonaws.com/deutsche-boerse-xetra-pds/2021-09-14/2021-09-14_BINS_XETR20.csv
2021-09-21 14:16:23,392 - xetra_jobs.s3.base_bucket - INFO - reading file https://s3.amazonaws.com/deutsche-boerse-xetra-pds/2021-09-14/2021-09-14_BINS_XETR21.csv
2021-09-21 14:16:23,564 - xetra_jobs.s3.base_bucket - INFO - reading file https://s3.amazonaws.com/deutsche-boerse-xetra-pds/2021-09-14/2021-09-14_BINS_XETR22.csv
2021-09-21 14:16:23,758 - xetra_jobs.s3.base_bucket - INFO - reading file https://s3.amazonaws.com/deutsche-boerse-xetra-pds/2021-09-14/2021-09-14_BINS_XETR23.csv
2021-09-21 14:16:23,909 - xetra_jobs.s3.base_bucket - INFO - reading file https://s3.amazonaws.com/deutsche-boerse-xetra-pds/2021-09-15/2021-09-15_BINS_XETR00.csv
2021-09-21 14:16:24,074 - xetra_jobs.s3.base_bucket - INFO - reading file https://s3.amazonaws.com/deutsche-boerse-xetra-pds/2021-09-15/2021-09-15_BINS_XETR01.csv
2021-09-21 14:16:24,239 - xetra_jobs.s3.base_bucket - INFO - reading file https://s3.amazonaws.com/deutsche-boerse-xetra-pds/2021-09-15/2021-09-15_BINS_XETR02.csv
2021-09-21 14:16:24,395 - xetra_jobs.s3.base_bucket - INFO - reading file https://s3.amazonaws.com/deutsche-boerse-xetra-pds/2021-09-15/2021-09-15_BINS_XETR03.csv
2021-09-21 14:16:24,566 - xetra_jobs.s3.base_bucket - INFO - reading file https://s3.amazonaws.com/deutsche-boerse-xetra-pds/2021-09-15/2021-09-15_BINS_XETR04.csv
2021-09-21 14:16:24,724 - xetra_jobs.s3.base_bucket - INFO - reading file https://s3.amazonaws.com/deutsche-boerse-xetra-pds/2021-09-15/2021-09-15_BINS_XETR05.csv
2021-09-21 14:16:24,903 - xetra_jobs.s3.base_bucket - INFO - reading file https://s3.amazonaws.com/deutsche-boerse-xetra-pds/2021-09-15/2021-09-15_BINS_XETR06.csv
2021-09-21 14:16:25,075 - xetra_jobs.s3.base_bucket - INFO - reading file https://s3.amazonaws.com/deutsche-boerse-xetra-pds/2021-09-15/2021-09-15_BINS_XETR07.csv
2021-09-21 14:16:25,513 - xetra_jobs.s3.base_bucket - INFO - reading file https://s3.amazonaws.com/deutsche-boerse-xetra-pds/2021-09-15/2021-09-15_BINS_XETR08.csv
2021-09-21 14:16:25,884 - xetra_jobs.s3.base_bucket - INFO - reading file https://s3.amazonaws.com/deutsche-boerse-xetra-pds/2021-09-15/2021-09-15_BINS_XETR09.csv
2021-09-21 14:16:26,258 - xetra_jobs.s3.base_bucket - INFO - reading file https://s3.amazonaws.com/deutsche-boerse-xetra-pds/2021-09-15/2021-09-15_BINS_XETR10.csv
2021-09-21 14:16:26,817 - xetra_jobs.s3.base_bucket - INFO - reading file https://s3.amazonaws.com/deutsche-boerse-xetra-pds/2021-09-15/2021-09-15_BINS_XETR11.csv
2021-09-21 14:16:27,158 - xetra_jobs.s3.base_bucket - INFO - reading file https://s3.amazonaws.com/deutsche-boerse-xetra-pds/2021-09-15/2021-09-15_BINS_XETR12.csv
2021-09-21 14:16:27,554 - xetra_jobs.s3.base_bucket - INFO - reading file https://s3.amazonaws.com/deutsche-boerse-xetra-pds/2021-09-15/2021-09-15_BINS_XETR13.csv
2021-09-21 14:16:27,984 - xetra_jobs.s3.base_bucket - INFO - reading file https://s3.amazonaws.com/deutsche-boerse-xetra-pds/2021-09-15/2021-09-15_BINS_XETR14.csv
2021-09-21 14:16:28,367 - xetra_jobs.s3.base_bucket - INFO - reading file https://s3.amazonaws.com/deutsche-boerse-xetra-pds/2021-09-15/2021-09-15_BINS_XETR15.csv
2021-09-21 14:16:28,812 - xetra_jobs.s3.base_bucket - INFO - reading file https://s3.amazonaws.com/deutsche-boerse-xetra-pds/2021-09-15/2021-09-15_BINS_XETR16.csv
2021-09-21 14:16:29,002 - xetra_jobs.s3.base_bucket - INFO - reading file https://s3.amazonaws.com/deutsche-boerse-xetra-pds/2021-09-15/2021-09-15_BINS_XETR17.csv
2021-09-21 14:16:29,160 - xetra_jobs.s3.base_bucket - INFO - reading file https://s3.amazonaws.com/deutsche-boerse-xetra-pds/2021-09-15/2021-09-15_BINS_XETR18.csv
2021-09-21 14:16:29,343 - xetra_jobs.s3.base_bucket - INFO - reading file https://s3.amazonaws.com/deutsche-boerse-xetra-pds/2021-09-15/2021-09-15_BINS_XETR19.csv
2021-09-21 14:16:29,542 - xetra_jobs.s3.base_bucket - INFO - reading file https://s3.amazonaws.com/deutsche-boerse-xetra-pds/2021-09-15/2021-09-15_BINS_XETR20.csv
2021-09-21 14:16:29,724 - xetra_jobs.s3.base_bucket - INFO - reading file https://s3.amazonaws.com/deutsche-boerse-xetra-pds/2021-09-15/2021-09-15_BINS_XETR21.csv
2021-09-21 14:16:29,914 - xetra_jobs.s3.base_bucket - INFO - reading file https://s3.amazonaws.com/deutsche-boerse-xetra-pds/2021-09-15/2021-09-15_BINS_XETR22.csv
2021-09-21 14:16:30,109 - xetra_jobs.s3.base_bucket - INFO - reading file https://s3.amazonaws.com/deutsche-boerse-xetra-pds/2021-09-15/2021-09-15_BINS_XETR23.csv
2021-09-21 14:16:30,364 - xetra_jobs.transformers.transformers - INFO - extracted data from source bucket
2021-09-21 14:16:30,873 - xetra_jobs.transformers.transformers - INFO - applied transformations to source data
2021-09-21 14:16:30,879 - xetra_jobs.transformers.transformers - INFO - saving transformed data into target bucket daily/20210915.parquet
2021-09-21 14:16:30,901 - xetra_jobs.s3.base_bucket - INFO - writing file to https://s3.amazonaws.com/xetra-report/daily/20210915.parquet
2021-09-21 14:16:31,564 - xetra_jobs.transformers.transformers - INFO - saved transformed data into target bucket daily/20210915.parquet
2021-09-21 14:16:31,574 - xetra_jobs.s3.base_bucket - INFO - reading meta file at https://s3.amazonaws.com/xetra-report/meta.csv
2021-09-21 14:16:31,700 - xetra_jobs.s3.base_bucket - INFO - writing file to https://s3.amazonaws.com/xetra-report/meta.csv
2021-09-21 14:16:31,865 - xetra_jobs.transformers.transformers - INFO - updated meta file
2021-09-21 14:16:31,895 - __main__ - INFO - xetra job finished for 2021-09-16
```

example log file for processed date

```
2021-09-20 20:43:46,538 - __main__ - INFO - xetra ETL job started for 2021-09-17
2021-09-20 20:43:46,538 - xetra_jobs.transformers.transformers - INFO - extracting xetra source data
2021-09-20 20:43:46,539 - xetra_jobs.s3.base_bucket - INFO - reading meta file at https://s3.amazonaws.com/xetra-report/meta.csv
2021-09-20 20:43:46,793 - xetra_jobs.transformers.transformers - INFO - input date exists in meta file, reading from target bucket
2021-09-20 20:43:46,801 - xetra_jobs.s3.base_bucket - INFO - reading file https://s3.amazonaws.com/xetra-report/daily/20210916.parquet
2021-09-20 20:43:47,094 - xetra_jobs.transformers.transformers - INFO - read data from target bucket
2021-09-20 20:43:47,094 - xetra_jobs.transformers.transformers - INFO - transformed dataframe, skip transformation
2021-09-20 20:43:47,095 - xetra_jobs.transformers.transformers - INFO - loaded data, skip loading
2021-09-20 20:43:47,108 - __main__ - INFO - ETL job finished for 2021-09-17
```
