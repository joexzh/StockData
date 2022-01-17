from datetime import datetime

import pandas as pd
import baostock as bs

import repo
import sdk


def pct_chg_sort(n: int, rm_kcb=True) -> pd.DataFrame:
    """
    n天前涨幅排名

    :param n:
    :param rm_kcb: 是否去除科创板
    :return: date, code, code_name, close, amount, pctChg
    """

    pd.set_option('display.max_rows', 1000)
    dt_fmt = '%Y-%m-%d'

    dt = datetime.now()
    # if dt.hour < 17 and n == 0:  # 当天未出数据
    #     raise ValueError(f'当天未出数据: {dt.strftime(dt_fmt)}')

    bs.login()

    start_date = ''
    day = 0
    # 获取一个月的交易日备用
    td_df = sdk.query_trade_dates_desc((dt + pd.DateOffset(months=-1)).strftime(dt_fmt), dt.strftime(dt_fmt))
    for i, td in td_df.iterrows():
        if td['is_trading_day'] == '1':
            if day == n:
                start_date = td['calendar_date']
                break
            day += 1

    if not start_date:
        raise ValueError(f'当天未出数据: {dt.strftime(dt_fmt)}')

    b_df = sdk.query_stock_basic()

    # 过滤证券类型=股票并且非退市, 只留下column=['code', 'code_name']
    b_df = b_df[(b_df.outDate == '') & (b_df.type == '1') & (b_df.status == '1')] \
        .drop(['ipoDate', 'outDate', 'type', 'status'], axis=1)

    kline_df = repo.query_history_k_by_date((start_date, start_date))

    # 过滤非st, 非停牌, 成交量大于5亿, 涨幅大于4%, 保留column
    kline_df = kline_df.loc[
        (kline_df['isST'] == 0) & (kline_df['tradestatus'] == 1) & (kline_df['amount'] > 500000000) & (kline_df['pctChg'] > 4),
        ['date', 'code', 'close', 'amount', 'pctChg']]
    if rm_kcb:  # 过滤非科创板
        kline_df = kline_df[~kline_df['code'].str.startswith('sh.688', na=False)]
    df = pd.merge(kline_df, b_df, how='left', on='code')  # 合并

    # reorder column to [date, code, code_name, close, amount, pctChg]
    cols = df.columns.tolist()
    cols = cols[:2] + cols[-1:] + cols[2:-1]
    df = df[cols]

    df = df.sort_values(['pctChg'], ascending=False).iloc[0:80].reset_index(drop=True)  # 涨幅%排序, 选前100只股票

    bs.logout()
    return df
