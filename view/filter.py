from datetime import datetime, timedelta
from decimal import localcontext, InvalidOperation
from typing import Callable

import pandas as pd

import db
import repo
import logging

from config import config


class DatePoints:
    date: pd.Timestamp
    date_10: pd.Timestamp
    date_30: pd.Timestamp
    date_60: pd.Timestamp
    date_120: pd.Timestamp
    date_180: pd.Timestamp
    date_240: pd.Timestamp

    def __init__(self, date: datetime):
        self.date = pd.Timestamp(date)
        self.date_10 = self.date - pd.DateOffset(days=10)
        self.date_30 = self.date - pd.DateOffset(days=30)
        self.date_60 = self.date - pd.DateOffset(days=60)
        self.date_120 = self.date - pd.DateOffset(days=120)
        self.date_180 = self.date - pd.DateOffset(days=180)
        self.date_240 = self.date - pd.DateOffset(days=240)


def get_valid_codes(today: str) -> list:
    return pd.read_sql(repo.sql_codes(today), db.db_engine)['code'].tolist()


def get_filtered_code_dict(today: datetime) -> dict:
    logging.info("选股: start to get break up codes...")

    date_str = today.strftime("%Y-%m-%d")
    codes = get_valid_codes(date_str)
    logging.info(f"选股: get {len(codes)} valid codes.")

    df = get_day_kline_from_codes(today, codes)
    logging.info(f"选股: get {df.shape[0]} k-lines")

    df.drop(columns=['id'], axis=1, inplace=True)
    df = df.sort_values(by='date')

    # 流通市值
    df['liquid'] = df['volume'] / (df['turn'] / 100) * df['close']
    # 振幅: abs(open - close)
    df['long'] = (df['open'] - df['close']).abs()

    dp = DatePoints(today)
    g = df.groupby(by='code', sort=False)

    fn_tuples = [("突破", is_break_up)]
    return create_code_dict(g, dp, fn_tuples)


def create_code_dict(g, dp: DatePoints, fn_tuples: list[tuple[str, Callable]]) -> dict:
    with localcontext() as ctx:
        ctx.traps[InvalidOperation] = False
        return dict([get_filter_codes(g, dp, fn_tuple) for fn_tuple in fn_tuples])


def get_filter_codes(g, dp: DatePoints, fn_tuple: tuple[str, Callable]) -> tuple[str, list[str]]:
    df = g.apply(lambda x: fn_tuple[1](x, dp))
    df = df[df]
    logging.info(f"选股: get {df.size} {fn_tuple[0]} codes")
    return fn_tuple[0], df.index.tolist()


def is_break_up(df: pd.DataFrame, dp: DatePoints) -> bool:
    """
    股票是否突破60天最高价
    :param df:
    :param dp:
    :return:
    """
    if df.shape[0] < 60:
        return False

    last_row = df.iloc[-1]

    if last_row['pctChg'] <= 0 or (last_row['open'] >= last_row['close']):
        return False

    close = last_row.close
    last_day = dp.date - pd.DateOffset(days=1)
    df_180 = df.loc[dp.date_180:last_day]
    max180 = df_180.close.max()
    min180 = df_180.close.min()
    valid_close = max180 < close < min180 * 1.5

    valid_value = last_row['liquid'] > 4000000000

    valid_long = last_row['long'] > df.loc[dp.date_60:last_day].long.mean()

    return valid_close and valid_value and valid_long


def get_day_kline_from_codes(today: datetime, codes: list) -> pd.DataFrame:
    start240 = today - timedelta(days=240)
    start_str = start240.strftime("%Y-%m-%d")
    end_str = today.strftime("%Y-%m-%d")
    return pd.read_sql(repo.sql_kline(start_str, end_str, codes), db.db_engine, index_col=['date'],
                       parse_dates=['date'])


if __name__ == '__main__':
    config.set_logger()
    now = datetime.now() - timedelta(days=1)
    code_dict = get_filtered_code_dict(now)
    print(code_dict)
    db.db_engine.dispose()
