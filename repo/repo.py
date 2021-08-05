import pandas as pd
import db
from .sql import *


def query_history_k_by_date(params) -> pd.DataFrame:
    """
    :param params: should be list or tuple: (datetime.date(2017, 4,1), datetime.date(2021, 7, 1))
    :return:
    """
    with db.create_ctx() as ctx:
        cols, rows = ctx.fetchall(sql_query_k_day_by_date, params)
        ctx.close()
        return pd.DataFrame(columns=cols, data=rows)
