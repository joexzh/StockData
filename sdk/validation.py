import baostock.data.resultset


def validate_rs(rs: baostock.data.resultset.ResultData, name: str):
    if rs.error_code != '0':
        raise IOError(f'baostock: {name}: {rs.error_msg}')
