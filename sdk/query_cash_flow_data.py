import baostock as bs
import pandas as pd

from sdk.validation import validate_rs


def query_cash_flow_data(code, year, quarter) -> pd.DataFrame:
    """
    http://baostock.com/baostock/index.php/%E5%AD%A3%E9%A2%91%E7%8E%B0%E9%87%91%E6%B5%81%E9%87%8F

    季频现金流量

    :param code: 股票代码，sh或sz.+6位数字代码，或者指数代码，如：sh.601398。sh：上海；sz：深圳。此参数不可为空；
    :param year: 统计年份，为空时默认当前年；
    :param quarter: 统计季度，为空时默认当前季度。不为空时只有4个取值：1，2，3，4。
    :return:
    """

    cash_flow_list = []
    rs_cash_flow = bs.query_cash_flow_data(code, year, quarter)
    validate_rs(rs_cash_flow, 'query_cash_flow_data')

    while (rs_cash_flow.error_code == '0') & rs_cash_flow.next():
        cash_flow_list.append(rs_cash_flow.get_row_data())
    result_cash_flow = pd.DataFrame(cash_flow_list, columns=rs_cash_flow.fields)
    return result_cash_flow
