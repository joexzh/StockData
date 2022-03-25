import baostock as bs
import pandas as pd

from sdk.validation import validate_rs


def query_performance_express_report(code, start_date, end_date) -> pd.DataFrame:
    """
    http://baostock.com/baostock/index.php/%E5%AD%A3%E9%A2%91%E4%B8%9A%E7%BB%A9%E5%BF%AB%E6%8A%A5

    季频公司业绩快报

    :param code: 股票代码，sh或sz.+6位数字代码，或者指数代码，如：sh.601398。sh：上海；sz：深圳。此参数不可为空；
    :param start_date: 开始日期，发布日期或更新日期在这个范围内；
    :param end_date: 结束日期，发布日期或更新日期在这个范围内。
    :return:
    """

    rs = bs.query_performance_express_report(code, start_date, end_date)
    validate_rs(rs, 'query_performance_express_report')

    result_list = []
    while (rs.error_code == '0') & rs.next():
        result_list.append(rs.get_row_data())
        # 获取一条记录，将记录合并在一起
    result = pd.DataFrame(result_list, columns=rs.fields)
    return result
