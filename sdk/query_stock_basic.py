import baostock as bs
import pandas as pd


def query_stock_basic(code="", code_name="") -> pd.DataFrame:
    """
    http://baostock.com/baostock/index.php/%E8%AF%81%E5%88%B8%E5%9F%BA%E6%9C%AC%E8%B5%84%E6%96%99

    证券基本资料

    :param code: A股股票代码，sh或sz.+6位数字代码，或者指数代码，如：sh.601398。sh：上海；sz：深圳。可以为空；
    :param code_name: 股票名称，支持模糊查询，可以为空。
    :return:
    """
    rs = bs.query_stock_basic(code, code_name)
    # rs = bs.query_stock_basic(code_name="浦发银行")  # 支持模糊查询
    print('query_stock_basic respond error_code:' + rs.error_code)
    print('query_stock_basic respond error_msg:' + rs.error_msg)

    # 打印结果集
    data_list = []
    while (rs.error_code == '0') & rs.next():
        # 获取一条记录，将记录合并在一起
        data_list.append(rs.get_row_data())
    result = pd.DataFrame(data_list, columns=rs.fields)
    return result
