import logging
from datetime import datetime, timedelta

import db
import sdk
import pandas as pd
import config
import repo

_table = 'history_k_data_d'


class Fetcher:
    def __init__(self) -> None:
        now = datetime.now()
        start_date = repo.last_date + timedelta(days=1)
        if start_date > now.date():
            raise ValueError(f'起止日期 {start_date} 不能大于今天 {now.date()}')
        if start_date == now.date() and now.hour < 17:
            raise ValueError(f'当天的数据尚未更新')
        self.start_date_str = start_date.strftime('%Y-%m-%d')
        logging.info(f'start date: {self.start_date_str}')

    def fetch_kline_data(self, codes: pd.Series) -> pd.DataFrame:
        kds = []
        for code in codes:
            kd = sdk.query_history_k_data_plus(code,
                                               ('date,code,open,high,low,close,preclose,volume,amount,'
                                                'turn,tradestatus,pctChg,peTTM,pbMRQ,psTTM,pcfNcfTTM,isST'),
                                               start_date=self.start_date_str,
                                               end_date=None,
                                               frequency='d',
                                               adjustflag='2')
            logging.info(f'code: {code}, rows: {kd.shape[0]}')
            kds.append(kd)
        logging.info(f'done fetch from {codes.iloc[0]} to {codes.iloc[-1]}')
        return pd.concat(kds)

    def fetch(self) -> pd.DataFrame:
        logging.info(f'start to retrieve {_table}')
        bcs = sdk.query_stock_basic()
        bcs = bcs[(bcs.outDate == '') & (bcs.type == '1') & (bcs.status == '1')]
        logging.info(f'total stocks: {bcs.shape[0]}')

        result = self.fetch_kline_data(bcs["code"])
        result.to_csv(r"D:\history_A_stock_k_data.csv", index=False)
        logging.info(f'total rows: {result.shape[0]}')
        return result


def save(result: pd.DataFrame):
    result = db.replace_empty_str_to_none(result)

    cnt = result.to_sql(_table, db.db_engine, if_exists='append', index=False, chunksize=10000, method='multi')
    logging.info(f'inserted {cnt} rows')

    return cnt


def fetch_and_save_k_day():
    f = Fetcher()
    result = f.fetch()
    logging.info(f'start to save to table `{_table}`, total rows: {result.shape[0]}')
    rowcnt = save(result)
    logging.info(f'saved {rowcnt} rows to table `{_table}`')


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
