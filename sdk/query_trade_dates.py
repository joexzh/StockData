import baostock as bs
import pandas as pd

from sdk.validation import validate_rs


def query_trade_dates(start_date=None, end_date=None) -> pd.DataFrame:
    """
    交易日查询
    http://baostock.com/baostock/index.php/Python_API%E6%96%87%E6%A1%A3#.E4.BA.A4.E6.98.93.E6.97.A5.E6.9F.A5.E8.AF.A2.EF.BC.9Aquery_trade_dates.28.29

    :param start_date: 开始日期，为空时默认为2015-01-01。
    :param end_date: 结束日期，为空时默认为当前日期。
    :return:
    """

    rs = bs.query_trade_dates(start_date, end_date)
    validate_rs(rs, 'query_trade_dates')

    data_list = []
    while (rs.error_code == '0') & rs.next():
        # 获取一条记录，将记录合并在一起
        data_list.append(rs.get_row_data())
    result = pd.DataFrame(data_list, columns=rs.fields)
    result['calendar_date'] = pd.to_datetime(result['calendar_date'])
    return result


def query_trade_dates_desc(start_date=None, end_date=None) -> pd.DataFrame:
    df = query_trade_dates(start_date, end_date)
    df = df.sort_values(['calendar_date'], ascending=False).reset_index(drop=True)
    return df
