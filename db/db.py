from contextlib import contextmanager

import mysql.connector
import pandas as pd

from config import mysql_config


def upset_sql(table, params):
    """
    Generate something like
    INSERT INTO stock_basic (`code`, `code_name`, `ipoDate`, `outDate`, `type`, `status`) VALUES(%(code)s, %(code_name)s, %(ipoDate)s,%(outDate)s,%(type)s,%(status)s)
ON DUPLICATE KEY UPDATE `code`="sh.600000",code_name="浦发银行",ipoDate="1999-11-10", outDate=NULL, type="1", `status`="1"

    :param table:
    :param params:
    :return:
    """
    sql = insert_sql(table, params)
    sql += ' ON DUPLICATE KEY UPDATE '

    for key in params:
        sql += '`' + key + '`=%(' + key + ')s,'
    sql = sql[:-1]
    return sql


def insert_sql(table, params):
    sql = 'INSERT INTO `' + table + '` ('
    for key in params:
        sql += '`' + key + '`,'
    sql = sql[:-1] + ') VALUES('

    for key in params:
        sql += '%(' + key + ')s,'
    sql = sql[:-1] + ')'
    return sql


def insert_many_sql(table, cols, vals):
    """
    INSERT INTO `table` (col1, col2, ...) VALUES (%s, %s, ...)

    :param table:
    :param cols:
    :param vals:
    :return:
    """
    sql = 'INSERT INTO `' + table + '` '
    if cols:
        sql += '('
        for col in cols:
            sql += '`' + col + '`,'
        sql = sql[:-1] + ') '
    sql += 'VALUES ('
    for _ in range(len(vals[0])):
        sql += '%s,'
    sql = sql[:-1] + ')'
    return sql


def delete_sql(table, params):
    sql = 'delete from `' + table + '` where '
    for key in params:
        sql += '`' + key + '`=%(' + key + ') and '
    sql = sql[:-5]
    return sql


class DbContext:
    """
    autocommit must remain False
    """

    def __init__(self, **db_config):
        self.conn = mysql.connector.connect(**db_config)

    def close(self):
        self.conn.close()

    def commit(self):
        self.conn.commit()

    def rollback(self):
        self.conn.rollback()

    def fetchall(self, sql, params):
        """
        fetch all query results
        
        :param sql: sql string with formatted like %s
        :param params: tuple or dict
        :return: list of tuple
        """
        cur = self.conn.cursor()
        cur.execute(sql, params)
        rows = cur.fetchall()
        cols = cur.column_names
        cur.close()
        return cols, rows

    def fetchone(self, sql, params):
        """
        fet one query result
        
        :param sql: sql string with formatted like %s
        :param params: tuple or dict
        :return: one tuple or None
        """
        cur = self.conn.cursor()
        cur.execute(sql, params)
        row = cur.fetchone()
        cols = cur.column_names
        cur.close()
        return cols, row

    def upset(self, table, params: dict):
        """
        Update or insert a row, base on the primary or unique key. You should manually commit when it's done.
        
        :param table: 
        :param params: a dict like {'column1', 'value1', 'column2', 'value2', ...}
        :return: None
        """
        if len(params) < 1 or not isinstance(params, dict):
            raise mysql.connector.DataError("参数错误", values=params)

        cur = self.conn.cursor()
        sql = upset_sql(table, params)
        cur.execute(sql, params)
        cur.close()

    def insert_one(self, table, params: dict):
        """
        Insert one, should commit by hand

        :param table:
        :param params: dict
        :return: rowcount
        """
        if len(params) < 1 or not isinstance(params, dict):
            raise mysql.connector.DataError("参数错误", values=params)
        cur = self.conn.cursor()
        sql = insert_sql(table, params)
        cur.execute(sql, params)
        cur.close()
        return cur.rowcount

    def insert_many(self, table, cols: list, vals: list):
        if not vals:
            return 0
        cur = self.conn.cursor()
        sql = insert_many_sql(table, cols, vals)
        cur.executemany(sql, vals)
        cur.close()
        return cur.rowcount

    def delete(self, table, params):
        """
        Delete rows, commit is needed.
        
        :param table: 
        :param params: dict like {'param1', 'value1', 'param2', 'value2', ...}
        :return: rowcount
        """
        cur = self.conn.cursor()
        sql = delete_sql(table, params)
        cur.execute(sql, params)
        rowcount = cur.rowcount
        cur.close()
        return rowcount


@contextmanager
def create_ctx():
    ctx = DbContext(**mysql_config)
    try:
        yield ctx
    finally:
        if ctx:
            ctx.close()


def replace_empty_str_to_none(df: pd.DataFrame) -> pd.DataFrame:
    """
    for mysql cannot auto convert '' to None for a number type
    :param df:
    :return:
    """
    return df.replace([''], [None])
