from datetime import date

import yaml

from .config import mysql_config, set_logger

config_file = r'E:\github\joexzh\StockData\config\config.yml'


def load_config_yml():
    with open(config_file, 'r') as f:
        return yaml.safe_load(f)


def save_config_yml(config_dict: dict):
    global config_yml
    config_yml = config_dict
    with open(config_file, 'w') as f:
        yaml.dump(config_dict, f)


config_yml = load_config_yml()


def set_k_day_last_updated(d: date):
    config_yml['k-day']['lastUpdated'] = d


def get_k_day_last_updated():
    return config_yml['k-day']['lastUpdated']
