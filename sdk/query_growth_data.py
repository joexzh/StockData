import baostock as bs
import pandas as pd


def query_growth_data(code, year, quarter) -> pd.DataFrame:
    """
    http://baostock.com/baostock/index.php/%E5%AD%A3%E9%A2%91%E6%88%90%E9%95%BF%E8%83%BD%E5%8A%9B

    季频成长能力

    :param code: 股票代码，sh或sz.+6位数字代码，或者指数代码，如：sh.601398。sh：上海；sz：深圳。此参数不可为空；
    :param year: 统计年份，为空时默认当前年；
    :param quarter: 统计季度，为空时默认当前季度。不为空时只有4个取值：1，2，3，4。
    :return:
    """

    growth_list = []
    rs_growth = bs.query_growth_data(code, year, quarter)
    while (rs_growth.error_code == '0') & rs_growth.next():
        growth_list.append(rs_growth.get_row_data())
    result_growth = pd.DataFrame(growth_list, columns=rs_growth.fields)
    return result_growth


