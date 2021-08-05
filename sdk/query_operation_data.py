import baostock as bs
import pandas as pd


def query_operation_data(code, year, quarter) -> pd.DataFrame:
    """
    http://baostock.com/baostock/index.php/%E5%AD%A3%E9%A2%91%E8%90%A5%E8%BF%90%E8%83%BD%E5%8A%9B

    季频营运能力

    :param code: 股票代码，sh或sz.+6位数字代码，或者指数代码，如：sh.601398。sh：上海；sz：深圳。此参数不可为空；
    :param year: 统计年份，为空时默认当前年；
    :param quarter: 统计季度，为空时默认当前季度。不为空时只有4个取值：1，2，3，4。
    :return:
    """

    operation_list = []
    rs_operation = bs.query_operation_data(code, year, quarter)
    while (rs_operation.error_code == '0') & rs_operation.next():
        operation_list.append(rs_operation.get_row_data())
    result_operation = pd.DataFrame(operation_list, columns=rs_operation.fields)
    return result_operation
