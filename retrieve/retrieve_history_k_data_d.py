import logging
import sys
from datetime import datetime

import db
import sdk
import pandas as pd

_table = 'history_k_data_d'


def fetch():
    try:
        now = datetime.now()
        start_date = (config.get_k_day_last_updated() + pd.DateOffset(days=1))
        if start_date > now.date():
            raise ValueError(f'起止日期 {start_date.date()} 不能大于今天 {now.date()}')
        if start_date == now.date() and now.hour < 17:
            raise ValueError(f'当天的数据尚未更新')

        start_date_str = start_date.strftime('%Y-%m-%d')

        sdk.login()

        logging.info(f'start to retrieve {_table}')

        bcs = sdk.query_stock_basic()
        bcs = bcs[(bcs.outDate == '') & (bcs.type == '1') & (bcs.status == '1')]

        kds = []
        for code in bcs['code']:
            kd = sdk.query_history_k_data_plus(code,
                                               ('date,code,open,high,low,close,preclose,volume,amount,'
                                                'turn,tradestatus,pctChg,peTTM,pbMRQ,psTTM,pcfNcfTTM,isST'),
                                               start_date=start_date_str,
                                               end_date=None,
                                               frequency='d',
                                               adjustflag='2')
            logging.info(f'code: {code}, rows: {kd.shape[0]}')
            kds.append(kd)

        result = pd.concat(kds)

        if bcs.shape[0] > result.shape[0]:
            raise ValueError(f'rows:{result.shape[0]} should more than stocks:{bcs.shape[0]}.')

        result.to_csv(r"D:\history_A_stock_k_data.csv", index=False)
        logging.info(f'total rows: {result.shape[0]}')
        return result
    finally:
        sdk.logout()


def save(result: pd.DataFrame):
    result = db.replace_empty_str_to_none(result)

    with db.create_ctx() as ctx:
        total_row_count = 0
        step = 10000
        for i in range(0, result.shape[0], step):
            ret_part = result.iloc[i:i + step, :]
            rowcnt = ctx.insert_many(_table, ret_part.columns.tolist(), ret_part.values.tolist())
            total_row_count += rowcnt
            logging.info(f'inserted {rowcnt} rows')
        ctx.commit()

    if result.shape[0] > 0:
        dts = pd.to_datetime(result['date'], infer_datetime_format=False).sort_values(ascending=False)
        d = dts.iloc[0].date()
        config.set_k_day_last_updated(d)
        config.save_config_yml(config.config_yml)

    return total_row_count


def fetch_and_save_k_day():
    result = fetch()
    logging.info(f'start to save table `{_table}`, total rows: {result.shape[0]}')
    rowcnt = save(result)
    logging.info(f'saved {rowcnt} rows to table `{_table}`')


if __name__ == '__main__':
    import config

    try:
        config.set_logger()

        fetch_and_save_k_day()
    except ValueError as e:
        logging.error(str(e))
    except Exception as e:
        raise e
