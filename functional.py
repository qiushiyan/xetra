import boto3
import pandas as pd
from datetime import date, datetime, timedelta
from io import BytesIO, StringIO

columns = ['ISIN', 'Mnemonic', 'SecurityDesc', 'SecurityType', 'Currency',
           'SecurityID', 'Date', 'Time', 'StartPrice', 'MaxPrice', 'MinPrice',
           'EndPrice', 'TradedVolume', 'NumberOfTrades']

source_bucket = "deutsche-boerse-xetra-pds"
target_bucket = "xetra-report"
date_format = "%Y-%m-%d"


def list_dates_after(input_date, date_format=date_format):
    prev_input_date = datetime.strptime(
        input_date, date_format).date() - timedelta(days=1)
    today = datetime.today().date()
    dates = [(prev_input_date + timedelta(days=x)).strftime(date_format)
             for x in range(0, (today-prev_input_date).days + 1)]
    return dates


def list_keys_by_date_prefix(bucket, date_prefix):
    keys = [obj.key for obj in bucket.objects.filter(Prefix=date_prefix)]
    return keys


def csv_to_df(bucket, key, decoding="utf-8", columns=columns):
    csv_obj = bucket.Object(key=key)\
        .get()\
        .get("Body")\
        .read()\
        .decode(decoding)
    data = StringIO(csv_obj)
    df = pd.read_csv(data)
    df = df.loc[:, columns]
    return df


def write_to_s3(df, key, bucket=target_bucket):
    out_buffer = BytesIO()
    df.to_parquet(out_buffer, index=False)
    bucket.put_object(Body=out_buffer.getvalue(), key=key)
    return True


def collect_df(bucket, dates):
    all_keys = []
    for date in dates:
        for key in list_keys_by_date_prefix(bucket, date):
            all_keys.append(key)
    df = pd.concat([csv_to_df(bucket, key)
                   for key in all_keys[0]], ignore_index=True)
    return df


def transform(df, input_date):
    df.dropna(inplace=True)
    df["opening_price"] = df.sort_values(["Time"]).groupby(
        ["ISIN", "Date"])["StartPrice"].transform("first")

    df["closing_price"] = df.sort_values(["Time"]).groupby(
        ["ISIN", "Date"])["StartPrice"].transform("last")

    df = df.groupby(["ISIN", "Date"], as_index=False).agg(
        opening_price=("opening_price", "min"),
        closing_price=("closing_price", "min"),
        min_price=("MinPrice", "min"),
        max_prce=("MaxPrice", "max"),
        daily_traded_volumn=("TradedVolume", "sum")
    )
    df["prev_closing_price"] = df.sort_values(
        by=["Date"]).groupby(["ISIN"])["closing_price"].shift(1)
    df["prev_closing_price_%"] = (
        df["closing_price"] - df["prev_closing_price"]) / df["prev_closing_price"] * 100
    df.drop(columns=["prev_closing_price"], inplace=True)
    return df[df.Date >= input_date]


s3 = boto3.resource('s3')
bucket = s3.Bucket(source_bucket)

dates = list_dates_after("2021-09-10")
df = collect_df(bucket, dates)
