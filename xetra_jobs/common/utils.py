from datetime import datetime


def is_weekday():
    today = datetime.today()


def is_weekday(date, date_format):
    if not isinstance(date, datetime):
        date = datetime.strptime(date, date_format)
    # Python's datetime library treats Monday as 0 and Sunday as 6
    return (0 <= date.weekday() < 5)


def str_to_date(date_str, date_format):
    return datetime.strptime(date_str, date_format)


def dates_to_strs(dates, date_format):
    return list(map(lambda d: d.strftime(date_format), dates))
