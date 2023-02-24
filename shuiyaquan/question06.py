#  -*- coding: utf-8 -*-
"""
@author:neo
@time:
"""
from __future__ import absolute_import, division, print_function, unicode_literals
import datetime
import pymongo
import backtrader as bt
import pandas as pd
from backtrader_plotting import Bokeh
from backtrader_plotting.schemes import Tradimo


class TestStrategy(bt.Strategy):
    params = (
        ('printlog', True),
    )

    def log(self, txt, dt=None, doprint=False):
        if self.params.printlog or doprint:
            dt = dt or self.datas[0].datetime.date(0)
            print('%s, %s' % (dt.isoformat(), txt))

    def __init__(self):
        self.dataclose = dict()
        # To keep track of pending orders and buy price/commission
        self.order = dict()
        self.buyprice = dict()
        self.buycomm = dict()

        self.MACD = dict()
        self.macd = dict()
        self.signal = dict()

        for data in self.datas:
            self.dataclose[data._name] = data.close
            self.order[data._name] = None
            self.buyprice[data._name] = None
            self.buycomm[data._name] = None
            self.MACD[data._name] = bt.indicators.MACD(data)
            self.macd[data._name] = self.MACD[data._name].macd
            self.signal[data._name] = self.MACD[data._name].signal

    # 交易状态检测
    def notify_order(self, order):
        if order.status in [order.Submitted, order.Accepted]:
            return
        if order.status in [order.Completed]:
            if order.isbuy():
                self.log(
                    '买进 %s 的价格 %.2f,账户值；%.2f,交易费用：%.2f' %
                    (order.data._name, order.executed.price, order.executed.value, order.executed.comm))
                self.buyprice[order.data._name] = order.executed.price
                self.buycomm[order.data._name] = order.executed.comm
            elif order.issell():
                self.log(
                    '卖出 %s 的价格 %.2f,账户值；%.2f,交易费用：%.2f' %
                    (order.data._name, order.executed.price, order.executed.value, order.executed.comm))
            self.bar_executed = len(self)
        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            self.log('交易取消,资金不足，交易拒接')

    # 交易完统计
    def notify_trade(self, trade):
        if not trade.isclosed:
            return
        self.log('利润：%.2f,总利润: %.2f' % (trade.pnl, trade.pnlcomm))

    def next(self):
        for data in self.datas:
            if self.getposition(data).size == 0:
                if self.macd[data._name][-1] < self.signal[data._name][-1]:
                    if self.macd[data._name][0] > self.signal[data._name][0]:
                        # macd金叉买入
                        self.order[data._name] = self.buy(data=data)
                        self.log('MACD金叉买入价格:%.2f' % self.dataclose[data._name][0])
            else:  # 有仓，执行卖策略
                if self.macd[data._name][-1] > self.signal[data._name][-1]:
                    if self.macd[data._name][0] < self.signal[data._name][-1]:
                        # 死叉，卖出
                        self.order[data._name] = self.sell(data=data)
                        self.log('MACD死叉卖出价格:%.2f' % self.dataclose[data._name][0])


if __name__ == '__main__':
    # 股票池
    stock_list = ['000001', '000002', '000004', ]
    start_cash = 1000000
    cerebro = bt.Cerebro()
    cerebro.addstrategy(TestStrategy)

    client = pymongo.MongoClient('mongodb://localhost:27017/')
    stock_db = client['stock_day']

    for stock in stock_list:
        table = stock_db[stock]
        df = pd.DataFrame(table.find())
        df.rename(columns={'日期': 'date', '开盘': 'open', '收盘': 'close', '最高': 'high', '最低': 'low', '成交量': 'volume'},
                  inplace=True)
        df = df[['date', 'open', 'close', 'high', 'low', 'volume']]
        df.index = pd.to_datetime(df['date'])
        df['openinterest'] = 0  # 持仓量归零
        data = bt.feeds.PandasData(dataname=df,
                                   fromdate=datetime.datetime(2020, 1, 1),
                                   todate=datetime.datetime(2023, 2, 22),
                                   )
        cerebro.adddata(data, name=stock)

    client.close()

    # broker设置资金、手续费
    cerebro.broker.setcash(start_cash)
    cerebro.broker.setcommission(commission=0.0015)
    # 设置买入设置，固定买卖数量
    cerebro.addsizer(bt.sizers.FixedSize, stake=100)
    cerebro.run()
    print("股票池：", stock_list)
    print('初始资金: %.2f , 最终的资金: %.2f' % (start_cash, cerebro.broker.getvalue()))

    b = Bokeh(style='bar', plot_mode='single', scheme=Tradimo())
    cerebro.plot(b)
