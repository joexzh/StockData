from datetime import datetime, timedelta
from decimal import localcontext, InvalidOperation

import pandas as pd

import db
import repo
import logging

from config import config


class DatePoints:
    date: pd.Timestamp
    date_30: pd.Timestamp
    date_60: pd.Timestamp
    date_120: pd.Timestamp
    date_240: pd.Timestamp

    def __init__(self, date: datetime):
        self.date = pd.Timestamp(date)
        self.date_30 = self.date - pd.DateOffset(days=30)
        self.date_60 = self.date - pd.DateOffset(days=60)
        self.date_120 = self.date - pd.DateOffset(days=120)
        self.date_240 = self.date - pd.DateOffset(days=240)


def is_break_up(df: pd.DataFrame, dp: DatePoints) -> bool:
    if df.shape[0] < 30:
        return False

    last_row = df.iloc[-1]

    if last_row['pctChg'] <= 0 or (last_row['open'] >= last_row['close']):
        return False

    close_sq = df['close']
    close = close_sq[-1]

    max30 = close_sq.loc[dp.date_30:].max()
    max60 = close_sq.loc[dp.date_60:dp.date_30].max()
    if max30 > max60:
        max60 = max30
    max120 = close_sq.loc[dp.date_120:dp.date_60].max()
    if max60 > max120:
        max120 = max60
    max240 = close_sq.loc[dp.date_240:dp.date_120].max()
    if max120 > max240:
        max240 = max120

    is_break = True if close >= min(max30, max60, max120, max240) else False
    return is_break


def get_valid_codes(today: str) -> list:
    return pd.read_sql(repo.sql_codes(today), db.db_engine)['code'].tolist()


def get_break_up_codes(today: datetime) -> list:
    logging.info("break_up: start to get break up codes...")

    date_str = today.strftime("%Y-%m-%d")
    codes = get_valid_codes(date_str)
    logging.info(f"break_up: get {len(codes)} valid codes.")

    df = get_day_kline_from_codes(today, codes)
    logging.info(f"break_up: get {df.shape[0]} k-lines")

    df.drop(columns=['id'], axis=1, inplace=True)
    df = df.sort_values(by='date')

    dp = DatePoints(today)

    with localcontext() as ctx:
        ctx.traps[InvalidOperation] = False

        g = df.groupby(by='code', sort=False).apply(
            lambda x: is_break_up(x, dp))
        g = g[g]
        logging.info(f"break_up: get {g.size} break codes")
        return g.index.tolist()


def get_day_kline_from_codes(today: datetime, codes: list) -> pd.DataFrame:
    start240 = today - timedelta(days=240)
    start_str = start240.strftime("%Y-%m-%d")
    end_str = today.strftime("%Y-%m-%d")
    return pd.read_sql(repo.sql_kline(start_str, end_str, codes), db.db_engine, index_col=['date'],
                       parse_dates=['date'])


if __name__ == '__main__':
    config.set_logger()
    now = datetime.now() - timedelta(days=0)
    cs = get_break_up_codes(now)
    print(cs)
    db.db_engine.dispose()
