import baostock as bs
import pandas as pd

from sdk.validation import validate_rs


def query_dupont_data(code, year, quarter) -> pd.DataFrame:
    """
    http://baostock.com/baostock/index.php/%E5%AD%A3%E9%A2%91%E6%9D%9C%E9%82%A6%E6%8C%87%E6%95%B0

    季频杜邦指数

    :param code: 股票代码，sh或sz.+6位数字代码，或者指数代码，如：sh.601398。sh：上海；sz：深圳。此参数不可为空；
    :param year: 统计年份，为空时默认当前年；
    :param quarter: 统计季度，为空时默认当前季度。不为空时只有4个取值：1，2，3，4。
    :return:
    """

    dupont_list = []
    rs_dupont = bs.query_dupont_data(code, year, quarter)
    validate_rs(rs_dupont, 'query_dupont_data')

    while (rs_dupont.error_code == '0') & rs_dupont.next():
        dupont_list.append(rs_dupont.get_row_data())
    result_profit = pd.DataFrame(dupont_list, columns=rs_dupont.fields)
    return result_profit
