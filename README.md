# Stock filter, 基于[baostock](https://www.baostock.com)接口

获取日k线到数据库:
> python retrieve.retrieve_history.py

n天前的涨幅排名, 非科创板, 非停牌, 非st, 成交额>5亿, 取前80:
> python history_app.py
