import logging
from datetime import datetime, timedelta

import db
import sdk
import pandas as pd
import config
import repo

_table = 'history_k_data_d'


def prepare_start_date():
    now = datetime.now()
    start_date = repo.last_date() + timedelta(days=1)
    if start_date > now.date():
        logging.info(f'{__name__}: 起止日期 {start_date} 不能大于今天 {now.date()} ')
        return False, ""
    if start_date == now.date() and now.hour < 17:
        logging.info(f'{__name__}: 当天的数据尚未更新')
        return False, ""
    start_date_str = start_date.strftime('%Y-%m-%d')
    logging.info(f'start date: {start_date_str}')
    return True, start_date_str


def fetch(start_date_str: str) -> pd.DataFrame:
    bcs = sdk.query_stock_basic()
    bcs = bcs[(bcs.outDate == '') & (bcs.type == '1') & (bcs.status == '1')]
    logging.info(f'{__name__}: total stocks: {bcs.shape[0]}')

    codes = bcs["code"]
    kds = []
    for code in codes:
        kd = sdk.query_history_k_data_plus(code,
                                           ('date,code,open,high,low,close,preclose,volume,amount,'
                                            'turn,tradestatus,pctChg,peTTM,pbMRQ,psTTM,pcfNcfTTM,isST'),
                                           start_date=start_date_str,
                                           end_date=None,
                                           frequency='d',
                                           adjustflag='2')
        logging.info(f'{__name__}: code: {code}, rows: {kd.shape[0]}')
        kds.append(kd)
    logging.info(f'{__name__}: done fetch from {codes.iloc[0]} to {codes.iloc[-1]}')
    result = pd.concat(kds)
    result.to_csv(r"D:\history_A_stock_k_data.csv", index=False)
    return result


def save(result: pd.DataFrame):
    result = db.replace_empty_str_to_none(result)
    cnt = result.to_sql(_table, db.db_engine, if_exists='append', index=False, chunksize=10000, method='multi')
    return cnt


def fetch_and_save_k_day():
    try:
        valid, date_str = prepare_start_date()
        if not valid:
            return
        result = fetch(date_str)
        logging.info(f'{__name__}: start to save to table `{_table}`, total rows: {result.shape[0]}')
        rowcnt = save(result)
        logging.info(f'{__name__}: saved {rowcnt} rows to table `{_table}`')
    except Exception as ex:
        logging.error(f"{__name__}: {ex}")


if __name__ == '__main__':
    try:
        config.set_logger()
        fetch_and_save_k_day()
    except ValueError as e:
        logging.error(str(e))
    except Exception as e:
        raise e
    finally:
        db.db_engine.dispose()
