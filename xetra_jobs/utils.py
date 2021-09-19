from datetime import datetime, timedelta


def is_weekday(date, date_format):
    if not isinstance(date, datetime):
        date = datetime.strptime(date, date_format)
    # Python's datetime library treats Monday as 0 and Sunday as 6
    return (0 <= date.weekday() < 5)


def str_to_date(date_str, date_format):
    return datetime.strptime(date_str, date_format)


def dates_to_strs(dates, date_format):
    return list(map(lambda d: d.strftime(date_format), dates))


def list_dates(input_date, date_format, single_day):
    """list a series of workdays after an input date

    :param input_date: tart date
    :param date_format: date format codes
    :param: if true, only return the previous workday and the start date

    returns:
        all workdays after the start date, the day prior to start date will also be included to calculate growth
    """

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
