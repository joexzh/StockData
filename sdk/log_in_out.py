from contextlib import contextmanager
import baostock as bs
from sdk.validation import validate_rs


def login():
    # 登陆系统
    lg = bs.login()
    validate_rs(lg, 'login')


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
