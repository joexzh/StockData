import datetime
import os
import subprocess

import time

import config
import yaml
import pandas as pd
import view
from retrieve import (fetch_and_save_k_day)
from contextlib import contextmanager
from repo import sql_kline, sql_codes

s = sql_kline(datetime.datetime.now())
print(s)
s1 = sql_codes(datetime.datetime.now())
print(s1)