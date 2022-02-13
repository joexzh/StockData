import asyncio
import logging
import subprocess
from datetime import datetime

import view
import config
from retrieve import fetch_and_save_k_day

doc = r'C:\USERS\ADMINISTRATOR\Documents'


def app():
    write_all_days(write_one_day)

    date = datetime.now()
    write_break_up_codes(date)

    p = next(gen_day_n_path())
    subprocess.call(fr'explorer /select, {p[1]}')


def write_one_day(n, path):
    df = view.pct_chg_sort(n)
    write(path, str(df['code'].tolist()))


def write(path, s):
    with open(path, 'w') as f:
        f.write(s)


def gen_day_n_path():
    yield 0, fr'{doc}\当天涨幅排序.txt'
    yield 1, fr'{doc}\1天前涨幅排序.txt'
    yield 2, fr'{doc}\2天前涨幅排序.txt'
    yield 3, fr'{doc}\3天前涨幅排序.txt'


def write_all_days(f):
    for i, path in gen_day_n_path():
        f(i, path)


def write_break_up_codes(date: datetime):
    codes = view.get_break_up_codes(date)
    write(fr'{doc}\突破.txt', str(codes))


if __name__ == '__main__':
    try:
        config.set_logger()
        asyncio.run(fetch_and_save_k_day())
        app()
    except ValueError as e:
        logging.error(str(e))
    except Exception as e:
        raise e

