from contextlib import contextmanager
import baostock as bs


def login():
    # 登陆系统
    lg = bs.login()
    # 显示登陆返回信息
    print('login respond error_code:' + lg.error_code)
    print('login respond  error_msg:' + lg.error_msg)


def logout():
    # 登出系统
    bs.logout()


@contextmanager
def bs_login_ctx():
    try:
        login()
        yield None
    finally:
        logout()