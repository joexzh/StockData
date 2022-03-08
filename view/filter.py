from datetime import timedelta, date
from decimal import localcontext, InvalidOperation

import pandas as pd
from pandas.core.groupby import DataFrameGroupBy

import db
import repo
import logging

from config import config


class TimestampPoints:
    ts: pd.Timestamp
    ts_10: pd.Timestamp
    ts_30: pd.Timestamp
    ts_60: pd.Timestamp
    ts_120: pd.Timestamp
    ts_180: pd.Timestamp
    ts_240: pd.Timestamp

    def __init__(self, to_ts: pd.Timestamp):
        self.ts = to_ts
        self.ts_10 = self.ts - pd.DateOffset(days=10)
        self.ts_30 = self.ts - pd.DateOffset(days=30)
        self.ts_60 = self.ts - pd.DateOffset(days=60)
        self.ts_120 = self.ts - pd.DateOffset(days=120)
        self.ts_180 = self.ts - pd.DateOffset(days=180)
        self.ts_240 = self.ts - pd.DateOffset(days=240)


def get_valid_codes(to_date: str) -> list:
    return pd.read_sql(repo.sql_codes(to_date), db.db_engine)['code'].tolist()


def get_filtered_code_dict(ndays=0) -> dict[str, list[str]]:
    """
    选股
    :param ndays: 0代表最新一天, 1代表加上前一天, 以此类推
    :return: 格式: {'突破0': ['sh.600000']}
    """
    to_date = repo.last_date
    logging.info("选股: start to get break up codes...")

    date_str = to_date.strftime("%Y-%m-%d")
    codes = get_valid_codes(date_str)
    logging.info(f"选股: get {len(codes)} valid codes.")

    df = get_day_kline_from_codes(to_date, codes, ndays)
    logging.info(f"选股: get {df.shape[0]} k-lines")

    df.drop(columns=['id'], axis=1, inplace=True)
    df = df.sort_values(by='date')

    # 流通市值
    df['liquid'] = df['volume'] / (df['turn'] / 100) * df['close']
    # 振幅: abs(open - close)
    df['long'] = (df['open'] - df['close']).abs()

    # 前n个交易日, reversed
    trade_ts_list: list[pd.Timestamp] = sorted(set(df.index), reverse=True)[:ndays + 1]
    tsp_list = [TimestampPoints(trade_ts) for trade_ts in trade_ts_list]

    g = df.groupby(by='code', sort=False)

    filter_dict = {}

    with localcontext() as ctx:
        ctx.traps[InvalidOperation] = False

        filter_dict.update(filter_breaks(g, trade_ts_list, tsp_list))

    return filter_dict


def filter_breaks(g: DataFrameGroupBy, trade_ts_list: list[pd.Timestamp], tsp_list: list[TimestampPoints]) \
        -> list[dict[str, list[str]]]:
    return {f"突破{i}": filter_break(g, trade_ts_list[i], tsp_list[i]) for i in range(len(tsp_list))}


def filter_break(g: DataFrameGroupBy, trade_ts: pd.Timestamp, tsp: TimestampPoints):
    df = g.apply(lambda x: is_break_up(x, trade_ts, tsp))
    df = df[df]
    return df.index.tolist()


def is_break_up(df: pd.DataFrame, trade_ts: pd.Timestamp, tsp: TimestampPoints) -> bool:
    """
    股票是否突破60天最高价
    :param df:
    :param trade_ts:
    :param tsp:
    :return:
    """
    df = df.loc[:trade_ts]
    if df.shape[0] < 60:
        return False

    last_row = df.iloc[-1]

    if last_row['pctChg'] <= 0 or (last_row['open'] >= last_row['close']):
        return False

    close = last_row.close
    last_day = tsp.ts - pd.DateOffset(days=1)
    df_180 = df.loc[tsp.ts_180:last_day]
    max180 = df_180.close.max()
    min180 = df_180.close.min()
    valid_close = max180 < close < min180 * 1.5

    valid_liquid = last_row['liquid'] > 4000000000

    return valid_close and valid_liquid


def get_day_kline_from_codes(to_date: date, codes: list, ndays=0) -> pd.DataFrame:
    days = 240 + ndays
    start240 = to_date - timedelta(days)
    start_str = start240.strftime("%Y-%m-%d")
    end_str = to_date.strftime("%Y-%m-%d")
    return pd.read_sql(repo.sql_kline(start_str, end_str, codes), db.db_engine, index_col=['date'],
                       parse_dates=['date'])


if __name__ == '__main__':
    config.set_logger()
    code_dict = get_filtered_code_dict(3)
    print(code_dict)
    db.db_engine.dispose()
