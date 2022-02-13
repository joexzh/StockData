import pandas as pd
import db


def query_history_k_by_date(start: str, end: str = None, codes: list = None) -> pd.DataFrame:
    """

    :param start: start date. Can't be None
    :param end: end date. Default None
    :param codes: stock codes. Default None
    :return:
    """

    def build_sql_and_param(_start, _end, _codes: list) -> (str, tuple):
        sql = 'SELECT * FROM `history_k_data_d` WHERE `date` >= %s'
        params = (_start,)
        if end:
            sql += ' AND `date` <= %s'
            params += (_end,)
        if _codes:
            sql += ' AND `code` in ('
            for _ in _codes:
                sql += '%s,'
            sql = sql[:-1] + ')'
            params += tuple(_codes)
        return sql, params

    with db.create_ctx() as ctx:
        _sql, _params = build_sql_and_param(start, end, codes)
        cols, rows = ctx.fetchall(_sql, _params)
        return pd.DataFrame(columns=cols, data=rows)


def query_valid_codes(date: str) -> pd.DataFrame:
    sql = 'select DISTINCT code from history_k_data_d where date=%s and isST=0'
    with db.create_ctx() as ctx:
        cols, rows = ctx.fetchall(sql, (date,))
        return pd.DataFrame(columns=cols, data=rows)