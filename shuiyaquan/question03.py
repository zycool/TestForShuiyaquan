#  -*- coding: utf-8 -*-
"""
@author:neo
@time:
"""
import akshare as ak
import pymongo
from pymongo.errors import DuplicateKeyError
import json
import time
import datetime as dt


def get_balance_sheet(last_year, year, stock_db):
    for a in range(last_year + 1, year):
        print("将拉取 {} 的资产负债表 年报。".format(a))
        sheet = stock_db["balance" + str(a)]
        sheet.create_index([("股票代码", pymongo.DESCENDING)], background=True, unique=True)
        sheet.create_index([("公告日期", pymongo.DESCENDING)], background=True)
        df = ak.stock_zcfz_em(date=str(a) + '1231')
        if not df.empty:
            data = json.loads(df.T.to_json()).values()
            sheet.insert_many(data)
    else:
        print("更新 {} 年 资产负责表 年报。".format(last_year))
        sheet = stock_db["balance" + str(last_year)]

        balance_df = ak.stock_zcfz_em(date=str(last_year) + '1231')
        if not balance_df.empty:
            data = json.loads(balance_df.T.to_json()).values()
            i = 0
            try:
                for d in data:
                    sheet.insert_one(d)
                    i = i + 1
            except DuplicateKeyError:
                pass
            finally:
                print("更新了 {} 条数据".format(i))


def get_profit_sheet(last_year, year, stock_db):
    for a in range(last_year + 1, year):
        print("将拉取 {} 的 利润表 年报。".format(a))
        sheet = stock_db["profit" + str(a)]
        sheet.create_index([("股票代码", pymongo.DESCENDING)], background=True, unique=True)
        sheet.create_index([("公告日期", pymongo.DESCENDING)], background=True)
        df = ak.stock_lrb_em(date=str(a) + '1231')
        if not df.empty:
            data = json.loads(df.T.to_json()).values()
            sheet.insert_many(data)
    else:
        print("更新 {} 年利润表 年报。".format(last_year))
        sheet = stock_db["profit" + str(last_year)]
        df = ak.stock_lrb_em(date=str(last_year) + '1231')
        if not df.empty:
            data = json.loads(df.T.to_json()).values()
            i = 0
            try:
                for d in data:
                    sheet.insert_one(d)
                    i = i + 1
            except DuplicateKeyError:
                pass
            finally:
                print("更新了 {} 条数据".format(i))


def get_cash_flow_sheet(last_year, year, stock_db):
    for a in range(last_year + 1, year):
        print("将拉取 {} 的 现金流量表 年报。".format(a))
        sheet = stock_db["cash_flow" + str(a)]
        sheet.create_index([("股票代码", pymongo.DESCENDING)], background=True, unique=True)
        sheet.create_index([("公告日期", pymongo.DESCENDING)], background=True)
        df = ak.stock_xjll_em(date=str(a) + '1231')
        if not df.empty:
            data = json.loads(df.T.to_json()).values()
            sheet.insert_many(data)
    else:
        print("更新 {} 年现金流量表 年报。".format(last_year))
        sheet = stock_db["cash_flow" + str(last_year)]
        df = ak.stock_xjll_em(date=str(last_year) + '1231')
        if not df.empty:
            data = json.loads(df.T.to_json()).values()
            i = 0
            try:
                for d in data:
                    sheet.insert_one(d)
                    i = i + 1
            except DuplicateKeyError:
                pass
            finally:
                print("更新了 {} 条数据".format(i))


def update_values(tables, year, stock_db):
    balance_l = []
    profit_l = []
    cash_flow_l = []
    last_year = 2008
    for x in tables:
        if x.startswith("balance"):
            balance_l.append(x)
        elif x.startswith("profit"):
            profit_l.append(x)
        if x.startswith("cash_flow"):
            cash_flow_l.append(x)
    if len(balance_l) > 0:
        last_year = int(balance_l[-1][-4:])
    get_balance_sheet(last_year, year, stock_db)
    if len(profit_l) > 0:
        last_year = int(profit_l[-1][-4:])
    get_profit_sheet(last_year, year, stock_db)
    if len(cash_flow_l) > 0:
        last_year = int(cash_flow_l[-1][-4:])
    get_cash_flow_sheet(last_year, year, stock_db)


def main():
    # 打开数据库
    client = pymongo.MongoClient('mongodb://localhost:27017/')
    # 链接Database，若没有，则插入数据时会自动创建数据库“stock_annual_report”
    stock_db = client['stock_annual_report']
    # 获取数据库stock_day下集合名称(即MySql中的数据表)
    tables = stock_db.list_collection_names(session=None)
    tables.sort()
    year = dt.datetime.today().year

    start = time.time()
    update_values(tables, year, stock_db)
    end = time.time()
    print("完成，共用时 {}。".format(end - start))

    client.close()


if __name__ == '__main__':
    main()
