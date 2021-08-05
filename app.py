import mysql.connector
import numpy

import retrieve
from config import *
import baostock as bs
import pandas as pd
import logging

config.set_logger()

retrieve.fetch_and_save_k_day()
