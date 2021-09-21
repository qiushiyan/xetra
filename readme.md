# xetra: a trading data ETL pipline

This repo contains example ETL jobs for processing daily trading data from the Deutsche Börse Group.

## The Problem

## Usage

```bash
python ./run.py configs/config.yaml
```

Example source data

```
#> # A tibble: 193,095 x 8
#>    ISIN         Date       Time   StartPrice MaxPrice MinPrice EndPrice TradedVolume
#>    <chr>        <date>     <time>      <dbl>    <dbl>    <dbl>    <dbl>        <dbl>
#>  1 AT0000A0E9W5 2021-09-16 07:00        22.5     22.5     22.5     22.5         3279
#>  2 DE000A0DJ6J9 2021-09-16 07:00        37.6     37.6     37.5     37.5         1281
#>  3 DE000A0D6554 2021-09-16 07:00        15.2     15.2     15.1     15.1         7066
#>  4 DE000A0D9PT0 2021-09-16 07:00       188.     188      188.     188            374
#>  5 DE000A0HN5C6 2021-09-16 07:00        53.0     53.0     53.0     53.0         9971
#>  6 DE000A0JL9W6 2021-09-16 07:00        53.8     53.8     53.4     53.7          676
#>  7 DE000A0LD2U1 2021-09-16 07:00        16.0     16.0     16.0     16.0         4160
#>  8 DE000A0LD6E6 2021-09-16 07:00        88.9     89.2     88.8     88.9         1098
#>  9 DE000A0S8488 2021-09-16 07:00        18.9     18.9     18.7     18.8         7719
#> 10 DE000A0WMPJ6 2021-09-16 07:00        24.7     24.7     24.7     24.7         2211
#> # ... with 193,085 more rows
```

Example processed data

```
#> # A tibble: 3,050 x 7
#>    isin      opening_price closing_price min_price max_price traded_volume   pct
#>    <chr>             <dbl>         <dbl>     <dbl>     <dbl>         <int> <dbl>
#>  1 AT00000F~          9.65          9.38      9.35      9.65          1324  0.52
#>  2 AT000060~         21.5          21.4      21.3      21.5           2486  0.37
#>  3 AT000060~         17.2          17.2      17.1      17.2            256  0.47
#>  4 AT000064~        103.          102.      101.      103.             429 -1.71
#>  5 AT000065~         35.2          35.0      35.0      35.5            180  0.2
#>  6 AT000065~         20.6          20.8      20.5      20.8            600  0.49
#>  7 AT000072~          7.57          7.35      7.35      7.57           203  2.85
#>  8 AT000073~         48.9          47.9      47.9      48.9            318 -0.16
#>  9 AT000074~         25.1          24.5      24.5      25.1            100  1.41
#> 10 AT000074~         48.6          50.1      48.6      50.6          14358  0.68
#> # ... with 3,040 more rows
```

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
