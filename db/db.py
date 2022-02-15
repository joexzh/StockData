import pandas as pd
from sqlalchemy import create_engine

from config import mysql_config


def replace_empty_str_to_none(df: pd.DataFrame) -> pd.DataFrame:
    """
    for mysql cannot auto convert '' to None for a number type
    :param df:
    :return:
    """
    return df.replace([''], [None])


def get_engine():
    user = mysql_config["user"]
    password = mysql_config["password"]
    host = mysql_config["host"]
    database = mysql_config["database"]
    return create_engine(f'mysql+mysqlconnector://{user}:{password}@{host}/{database}')


db_engine = get_engine()
