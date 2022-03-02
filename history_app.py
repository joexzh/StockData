import logging
import subprocess

import db
import view
import config
import sdk
from retrieve import fetch_and_save_k_day

doc = r'C:\USERS\ADMINISTRATOR\Documents'


def change_sort_ndays(n: int):
    dfs = view.pct_chg_sort(n)
    path_gen = gen_path_ndays(len(dfs))
    for df in dfs:
        write(next(path_gen), str(df['code'].tolist()))

    p = next(gen_path_ndays(1))
    subprocess.call(fr'explorer /select, {p}')


def write(path, s):
    with open(path, 'w') as f:
        f.write(s)


def gen_path_ndays(n: int):
    for i in range(n):
        if i == 0:
            yield fr'{doc}\当天涨幅排序.txt'
        else:
            yield fr'{doc}\{i}天前涨幅排序.txt'


def filter_codes():
    code_dict = view.get_filtered_code_dict()
    for key in code_dict:
        write(fr'{doc}\{key}.txt', str(code_dict[key]))


if __name__ == '__main__':
    try:
        with sdk.bs_login_ctx():
            config.set_logger()
            fetch_and_save_k_day()
            change_sort_ndays(4)
            filter_codes()
    except ValueError as e:
        logging.error(str(e))
    except Exception as e:
        raise e
    finally:
        db.db_engine.dispose()
