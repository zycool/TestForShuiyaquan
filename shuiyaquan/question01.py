#  -*- coding: utf-8 -*-
"""
@author:neo
@time:
"""
import akshare as ak
import pymongo
import json
import time
import datetime as dt
import math
import threading


def get_all_stocks():
    """
    获取沪深京A股所有股票的所有代码、名称、交易所
    :return:
        pandas.DataFrame: 返回的DataFrame包含3列, ["代码","名称","交易所"]
    """
    df = ak.stock_zh_a_spot_em()
    df = df[['代码', '名称']]
    # 添加新列'交易所',并将其默认值设置为'SZ'
    df.loc[:, '交易所'] = 'SZ'
    df.loc[df['代码'].str.startswith('6'), '交易所'] = 'SH'
    df.loc[df['代码'].str.startswith('8'), '交易所'] = 'BJ'
    df.loc[df['代码'].str.startswith('300'), '交易所'] = 'CYB'
    df.loc[df['代码'].str.startswith('688'), '交易所'] = 'KCB'
    df.loc[df['代码'].str.startswith('4'), '交易所'] = 'SB'
    return df


def get_and_insert(m, job_per_thread, remainder, count_thread, df_codes, stock_db, tables):
    """
    工作线程
    :param m: 当前线程编号
    :param job_per_thread: 每个线程所需处理的股票数
    :param remainder: 最后一个线所需处理的股票数
    :param count_thread: 线程总量
    :param df_codes: 含有股票代码的df
    :param stock_db: 数据库游标
    :param tables: 当前数据库下集合名称列表
    :return:
    """
    date_start = "19700101"  # 设定默认的取数初始日期
    count_job = remainder if m == (count_thread - 1) else job_per_thread
    for i in range(0, count_job):
        code = df_codes.iloc[m * job_per_thread + i, 0]
        stock = stock_db[code]
        if code in tables:
            # 数据更新
            results = list(stock.find().sort('日期', pymongo.DESCENDING).limit(1))
            if len(results) > 0:  # 不是空表，就更新初始日期为最近K线日期
                date_start = results[0]['日期'].replace('-', '')
        else:  # 没拉取过这只票的数据
            stock.create_index([("日期", pymongo.DESCENDING)], background=True, unique=True)
        date_end = dt.datetime.now().strftime('%Y%m%d')
        # 只有初始日期比当前日期小才需要拉取数据
        if dt.datetime.strptime(date_start, '%Y%m%d') < dt.datetime.strptime(date_end, '%Y%m%d'):
            # 自最近那个K线的第二天开始更新
            date_start = dt.datetime.strptime(date_start, '%Y%m%d') + dt.timedelta(days=1)
            for t in range(1, 6):  # 网络拉取数据，并写入数据库，试错5次
                try:
                    klines = ak.stock_zh_a_hist(code, "daily", date_start.strftime('%Y%m%d'), date_end, "hfq")
                    if not klines.empty:
                        data = json.loads(klines.T.to_json()).values()
                        stock.insert_many(data)
                        print("{} 数据插入完毕，截止日期：{}".format(code, date_end))
                    break
                except Exception as e:
                    print(e)
                    # todo:可以把异常发至邮箱或钉钉
                    print("拉取 {} 初始数据出错，第 {}/5 次，后面会休息 3 秒。".format(code, t))
                    time.sleep(3)


def main():
    # 打开数据库
    client = pymongo.MongoClient('mongodb://localhost:27017/')
    # 链接Database，若没有，则插入数据时会自动创建数据库“stock_day”
    stock_db = client['stock_day']
    # 获取数据库stock_day下集合名称(即MySql中的数据表)
    tables = stock_db.list_collection_names(session=None)

    start = time.time()
    df_codes = get_all_stocks()
    print("股票总数是：{}".format(len(df_codes)))

    job_per_thread = 200  # 每个线程处理多少个股票数据
    remainder = len(df_codes) % job_per_thread  # 余数，最后一个线程处理多少个股票数据
    count_thread = math.ceil(len(df_codes) / job_per_thread)  # 线程数量
    print("每个线程处理 {} 个股票数据".format(job_per_thread))
    print("线程数量 {}".format(count_thread))
    print("最后一个线程处理 {} 个股票数据".format(remainder))
    threads = []
    for m in range(0, count_thread):
        t = threading.Thread(target=get_and_insert,
                             args=(m, job_per_thread, remainder, count_thread, df_codes, stock_db, tables))
        threads.append(t)
        t.start()
    for t in threads:
        t.join()

    end = time.time()
    print("完成，共用时 {}。".format(end - start))

    client.close()


if __name__ == '__main__':
    main()
