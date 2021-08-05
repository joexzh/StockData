import time
from datetime import datetime

import sdk
import pandas as pd


def avg_roe(n) -> pd.DataFrame:
    """
    n年平均roe, 涨幅排名

    :return:
    """
    sdk.login()

    df = sdk.query_stock_basic()
    df = df[(df.outDate == '') & (df.type == '1') & (df.status == '1')].drop(['outDate', 'type'], axis=1)

    dp_list = []
    change_list = []
    now = datetime.now()
    pre = now + pd.DateOffset(years=-abs(n))

    for code in df['code']:

        # roe
        for year in range(pre.year, now.year):

            dp_df = sdk.query_dupont_data(code, year, 4)  # 只取第四季度

            if not dp_df.empty:
                dp_list = dp_list.append(dp_df)

        # 涨幅
        # TODO
        start_date = pre.strftime('%Y-%m%-d')
        end_date = now.strftime('%Y-%m-%d')
        chg_df = sdk.query_history_k_data_plus(code, fields='code,close,amount,turn,peTTM', start_date=start_date,
                                               end_date=end_date, frequency='d', adjustflag='2')
        change_list = change_list.append(chg_df)

    dp_df = pd.concat(dp_list)
    chg_df = pd.concat(change_list)

    # TODO 转换类型 to float

    dp_df = dp_df.groupby  # TODO groupby code, mean std roe

    # TODO 处理chg_df: close涨幅, 平均amount, 平均turn, 最后一天的peTTM

    # TODO 因为需要code_name, df join dp_df join chg_df, 对齐code, sort close涨幅

    sdk.logout()

    return df
