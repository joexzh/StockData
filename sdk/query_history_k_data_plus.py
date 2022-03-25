import baostock as bs
import pandas as pd

from sdk.validation import validate_rs


def query_history_k_data_plus(code: str, fields: str, start_date: str = None, end_date: str = None,
                              frequency: str = 'd',
                              adjustflag: str = '2') -> pd.DataFrame:
    """
    http://baostock.com/baostock/index.php/A%E8%82%A1K%E7%BA%BF%E6%95%B0%E6%8D%AE

    获取历史A股K线数据

    :return: pandas的DataFrame类型。
    """
    rs = bs.query_history_k_data_plus(code, fields, start_date, end_date, frequency, adjustflag)
    validate_rs(rs, 'query_history_k_data_plus')

    data_list = []
    while (rs.error_code == '0') & rs.next():
        # 获取一条记录，将记录合并在一起
        data_list.append(rs.get_row_data())
    result = pd.DataFrame(data_list, columns=rs.fields)
    return result
