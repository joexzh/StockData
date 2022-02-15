import logging

mysql_config = {
    'user': 'root',
    'password': '199013fankaistar',
    'host': 'localhost',
    'database': 'stock_market',
    'use_pure': False
}


def set_logger():
    logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s',
                        datefmt='%Y-%m-%d %I:%M:%S%p',
                        level=logging.DEBUG)
