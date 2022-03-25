import baostock as bs
import pandas as pd

from sdk.validation import validate_rs


def query_forecast_report(code, start_date, end_date) -> pd.DataFrame:
    """
    http://baostock.com/baostock/index.php/%E5%AD%A3%E9%A2%91%E4%B8%9A%E7%BB%A9%E9%A2%84%E5%91%8A

    季频公司业绩预告

    :param code: 股票代码，sh或sz.+6位数字代码，或者指数代码，如：sh.601398。sh：上海；sz：深圳。此参数不可为空；
    :param start_date: 开始日期，发布日期或更新日期在这个范围内；
    :param end_date: 结束日期，发布日期或更新日期在这个范围内。
    :return:
    """

    rs_forecast = bs.query_forecast_report(code, start_date, end_date)
    validate_rs(rs_forecast, 'query_forecast_report')

    rs_forecast_list = []
    while (rs_forecast.error_code == '0') & rs_forecast.next():
        # 分页查询，将每页信息合并在一起
        rs_forecast_list.append(rs_forecast.get_row_data())
    result_forecast = pd.DataFrame(rs_forecast_list, columns=rs_forecast.fields)
    return result_forecast
