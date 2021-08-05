import subprocess
import view

doc = r'C:\USERS\ADMINISTRATOR\Documents'


def run():
    multi(once)
    p = next(gen_path())
    subprocess.call(fr'explorer /select, {p[1]}')


def once(n, path):
    df = view.pct_chg_sort(n)
    write(path, str(df['code'].tolist()))


def write(path, s):
    with open(path, 'w') as f:
        f.write(s)


def gen_path():
    yield 0, fr'{doc}\当天涨幅排序.txt'
    yield 1, fr'{doc}\1天前涨幅排序.txt'
    yield 2, fr'{doc}\2天前涨幅排序.txt'
    yield 3, fr'{doc}\3天前涨幅排序.txt'


def multi(f):
    for i, path in gen_path():
        f(i, path)


if __name__ == '__main__':
    run()
