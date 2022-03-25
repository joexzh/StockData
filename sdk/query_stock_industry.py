import baostock as bs
import pandas as pd

from sdk.validation import validate_rs


def query_stock_industry() -> pd.DataFrame:
    """
    http://baostock.com/baostock/index.php/%E8%A1%8C%E4%B8%9A%E5%88%86%E7%B1%BB

    行业分类
    :return:
    """

    rs = bs.query_stock_industry()
    validate_rs(rs, 'query_stock_industry')

    # 打印结果集
    industry_list = []
    while (rs.error_code == '0') & rs.next():
        # 获取一条记录，将记录合并在一起
        industry_list.append(rs.get_row_data())
    result = pd.DataFrame(industry_list, columns=rs.fields)
    return result
