import baostock as bs
import pandas as pd


def query_profit_data(code: str, year: int, quarter: int) -> pd.DataFrame:
    """
    http://baostock.com/baostock/index.php/%E5%AD%A3%E9%A2%91%E7%9B%88%E5%88%A9%E8%83%BD%E5%8A%9B

    季频盈利能力

    :param code: 股票代码，sh或sz.+6位数字代码，或者指数代码，如：sh.601398。sh：上海；sz：深圳。此参数不可为空；
    :param year: 统计年份，为空时默认当前年；
    :param quarter: 统计季度，可为空，默认当前季度。不为空时只有4个取值：1，2，3，4。
    :return:
    """

    # 查询季频估值指标盈利能力
    profit_list = []
    rs_profit = bs.query_profit_data(code, year, quarter)
    while (rs_profit.error_code == '0') & rs_profit.next():
        profit_list.append(rs_profit.get_row_data())
    result_profit = pd.DataFrame(profit_list, columns=rs_profit.fields)
    return result_profit
