import datetime
from typing import Optional

import pandas as pd
from sqlalchemy import Table, BIGINT, Column, Date, VARCHAR, DECIMAL, BigInteger, Float, MetaData, select
from sqlalchemy.dialects.mysql import TINYINT

import db

metadata = MetaData()
kline = Table(
    'history_k_data_d', metadata,
    Column('id', BIGINT, primary_key=True),
    Column('date', Date, nullable=False, index=True, comment='交易所行情日期'),
    Column('code', VARCHAR(255), nullable=False, index=True, comment='证券代码'),
    Column('open', DECIMAL(20, 4), comment='今开盘价格'),
    Column('high', DECIMAL(20, 4), comment='最高价'),
    Column('low', DECIMAL(20, 4), comment='最低价'),
    Column('close', DECIMAL(20, 4), comment='今收盘价'),
    Column('preclose', DECIMAL(20, 4), comment='昨日收盘价'),
    Column('volume', BigInteger, comment='成交数量'),
    Column('amount', DECIMAL(20, 4), comment='成交金额'),
    Column('turn', Float(20), comment='换手率'),
    Column('tradestatus', TINYINT, comment='交易状态'),
    Column('pctChg', Float(20), comment='涨跌幅（百分比）'),
    Column('peTTM', Float(20), comment='滚动市盈率'),
    Column('pbMRQ', Float(20), comment='市净率'),
    Column('psTTM', Float(20), comment='滚动市销率'),
    Column('pcfNcfTTM', Float(20), comment='\t滚动市现率'),
    Column('isST', TINYINT, comment='是否ST股，1是，0否'),
    comment='http://baostock.com/baostock/index.php/A%E8%82%A1K%E7%BA%BF%E6%95%B0%E6%8D%AE'
)


def sql_kline(start: str, end: Optional[str] = None, codes: Optional[list] = None):
    sql = select(kline).where(kline.c.date >= start)
    if end:
        sql = sql.where(kline.c.date <= end)
    if codes:
        sql = sql.where(kline.c.code.in_(codes))
    return sql


def sql_codes(to_date: str):
    sql = select(kline.c.code).distinct().where(kline.c.date == to_date).where(kline.c.isST == 0)
    return sql


last_date_sql = "SELECT DISTINCT date FROM history_k_data_d ORDER BY date DESC LIMIT 1"


def last_date() -> datetime.date:
    df = pd.read_sql(last_date_sql, db.db_engine, parse_dates=['date'])
    if df.shape[0] == 0:
        raise ValueError("history_k_data_d last date empty")
    return df.iloc[0].date.to_pydatetime().date()
