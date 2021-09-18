import boto3
import pandas as pd
from datetime import datetime, timedelta
from io import BytesIO, StringIO

columns = ['ISIN', 'Date', 'Time', 'StartPrice', 'MaxPrice', 'MinPrice',
           'EndPrice', 'TradedVolume']

source_bucket = "deutsche-boerse-xetra-pds"
target_bucket = "xetra-report"
date_format = "%Y-%m-%d"


def is_weekday(date, date_format=date_format):
    if not isinstance(date, datetime):
        date = datetime.strptime(date, date_format=date_format)
    return date.weekday() <= 4


def str_to_date(date_str, date_format):
    return datetime.strptime(date_str, date_format)


def dates_to_strs(dates, date_format=date_format):
    return list(map(lambda d: d.strftime(date_format), dates))


def list_dates(input_date, date_format=date_format, single_day=True):
    input_date = str_to_date(input_date, date_format)
    # if input is monday, should go back to last friday
    if input_date.weekday() == 0:
        back_days = timedelta(3)
    elif input_date.weekday() == 6:
        back_days = timedelta(2)
    # for saturday and weekdays, only need to go back one day
    else:
        back_days = timedelta(1)
    prev_input_date = input_date - back_days
    if single_day:
        return dates_to_strs([prev_input_date, input_date])
    else:
        today = datetime.today()
        dates = [(prev_input_date + timedelta(days=x)) for x in range(0, (today -
                                                                          prev_input_date).days + 1) if is_weekday((prev_input_date + timedelta(days=x)))]
        return dates_to_strs(dates)


def list_keys_by_date_prefix(bucket, date_prefix):
    keys = [obj.key for obj in bucket.objects.filter(Prefix=date_prefix)]
    return keys


def csv_to_df(bucket, key, decoding="utf-8", columns=columns):
    try:
        csv_obj = bucket.Object(key=key)\
            .get()\
            .get("Body")\
            .read()\
            .decode(decoding)
        rows = len(csv_obj.strip().split("\n"))
        if rows >= 1:
            data = StringIO(csv_obj)
            df = pd.read_csv(data, usecols=columns)
        else:
            df = pd.DataFrame(columns=columns)
    except:
        df = pd.DataFrame(columns=columns)
    return df


def df_to_s3(bucket, df, key):
    out_buffer = StringIO()
    df.to_parquet(out_buffer, index=False)
    bucket.put_object(Body=out_buffer.getvalue(), Key=key)
    return True


def collect_df(bucket, dates):
    all_keys = []
    for date in dates:
        for key in list_keys_by_date_prefix(bucket, date):
            all_keys.append(key)
    df = pd.concat([csv_to_df(bucket, key)
                   for key in all_keys], ignore_index=True)
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
    df["pct"] = (
        df["closing_price"] - df["prev_closing_price"]) / df["prev_closing_price"] * 100
    df.drop(columns=["prev_closing_price"], inplace=True)
    return df[df.Date >= input_date]


def load(bucket, df, date):
    key = f"daily/{date}.parquet"
    df_to_s3(bucket, df, key)
    return True


s3 = boto3.resource('s3')
bucket = s3.Bucket(source_bucket)

dates = list_dates("2021-09-10")
df = collect_df(bucket, dates)
