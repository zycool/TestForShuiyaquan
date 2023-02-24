#  -*- coding: utf-8 -*-
"""
@author:neo
@time:
"""
import akshare as ak
import pandas as pd
import numpy as np
import pymongo
from pymongo.errors import DuplicateKeyError
import json
import time
import datetime as dt
import math
import threading
from pyecharts import options as opts
from pyecharts.charts import Bar


def render_charts(res_df, title, subtitle):
    c = (
        Bar(init_opts=opts.InitOpts(page_title=title,
                                    width="1500px",
                                    height="750px",
                                    ))
        .add_xaxis(res_df['date'].tolist())
        .add_yaxis("涨停数", res_df['total'].tolist(), label_opts=opts.LabelOpts(is_show=False))
        .set_global_opts(
            title_opts=opts.TitleOpts(title=title, subtitle=subtitle),
            datazoom_opts=opts.DataZoomOpts(),
            toolbox_opts=opts.ToolboxOpts(),
            legend_opts=opts.LegendOpts(is_show=False),
        )
        .render("涨停数量分析结果.html")

    )


def tool_trade_date_hist(num: int = 100, end_date: str = ""):
    """
    获取 num 个交易日列表
    :param num: 整数，默认取前面100个交易日历
    :param end_date:最后交易日，格式 '20230223'，没有则为当天
    :return:
    """
    if not end_date:
        end_date = dt.datetime.now().strftime('%Y%m%d')
    trade_date = ak.tool_trade_date_hist_sina()
    trade_date = trade_date['trade_date'].apply(lambda x: x.strftime('%Y%m%d'))
    trade_date = np.array(trade_date)
    n1 = np.argwhere(trade_date == str(end_date))[0][0] + 1
    dates = trade_date[n1 - num: n1]
    return dates


def sum_by_date(date: str = '20230223'):
    """
    取得指定交易日的涨停板数量，去除新股连续涨停，同时也去除了连板数向前1天停牌的
    :param date:指定交易日
    :return: 指定交易日的涨停板数量
    """
    dates = tool_trade_date_hist(num=50, end_date=date)
    # 东方财富网-行情中心-涨停板行情-涨停股池
    zt_df = ak.stock_zt_pool_em(date=date)
    # 剔除当日新股
    zt_df = zt_df[~zt_df['名称'].str.startswith('N')]
    sum_r, sum_h = zt_df.shape

    lb_df = zt_df[zt_df['连板数'] > 1]
    del_indexes = []
    for ind in lb_df.index:
        code = lb_df.loc[ind]['代码']
        times = lb_df.loc[ind]['连板数']
        try:
            # 比连板数向前取1天的数据
            hist_df = ak.stock_zh_a_hist(symbol=code, period="daily", start_date=dates[-1 - times:][0],
                                         end_date=dates[-1],
                                         adjust="hfq")
            r, h = hist_df.shape
            if times == r:
                # 去除新股连续涨停，同时也去除了连板数向前1天停牌的
                del_indexes.append(ind)
        except Exception as e:
            time.sleep(2)
    sum_day = sum_r - len(del_indexes)
    return pd.DataFrame({"date": date, "total": sum_day}, index=['date', ])


def thread_sum(m, job_per_thread, trade_day_list, table, date_list_form_db):
    for i in range(0, job_per_thread):
        the_date = trade_day_list[m * job_per_thread + i]
        print("处理 {} 的数据...".format(the_date))
        if the_date in date_list_form_db:
            print("{} 的数据在数据库中有，直接select".format(the_date))
        else:
            total_df = sum_by_date(date=the_date)
            data = json.loads(total_df.T.to_json()).values()
            try:
                for d in data:
                    table.insert_one(d)
            except DuplicateKeyError:
                pass


def main():
    trade_day = 250
    trade_day_list = tool_trade_date_hist(trade_day, end_date="")
    print("汇总过去 {} 交易日的涨停板数量".format(trade_day))
    job_per_thread = 50  # 每个线程处理多少个股票数据
    count_thread = math.ceil(trade_day / job_per_thread)  # 线程数量

    # 打开数据库
    client = pymongo.MongoClient('mongodb://localhost:27017/')
    # 链接Database，若没有，则插入数据时会自动创建数据库“stock_annual_report”
    stock_db = client['stock_other']
    table = stock_db["data_stock_zt"]
    table.create_index([("date", pymongo.DESCENDING)], background=True, unique=True)
    res_df = pd.DataFrame(table.find().sort('date', pymongo.DESCENDING))
    date_list_form_db = res_df['date'].tolist()

    start = time.time()
    threads = []
    for m in range(0, count_thread):
        t = threading.Thread(target=thread_sum, args=(m, job_per_thread, trade_day_list, table, date_list_form_db))
        threads.append(t)
        t.start()
    for t in threads:
        t.join()
    # 有可能有新增加的数据插入，所以要重新取一次
    res_df = pd.DataFrame(table.find().sort('date', pymongo.ASCENDING))
    ll_df = pd.DataFrame(trade_day_list, columns=['date'])
    df = pd.merge(res_df, ll_df, on=['date', ])
    title = "过去 {} 个交易日A股涨停数量分析".format(trade_day)
    subtitle = "去除新股连续涨停的，去除停牌股票复牌之后连续涨停的"
    render_charts(df, title, subtitle)

    end = time.time()
    print("完成，共用时 {}。".format(end - start))
    client.close()


if __name__ == '__main__':
    main()
