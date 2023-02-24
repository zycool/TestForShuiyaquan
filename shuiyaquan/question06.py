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
        ('maperiod', 15),
        ('printlog', True),
    )

    def log(self, txt, dt=None):
        dt = dt or self.datas[0].datetime.date(0)
        print('%s, %s' % (dt.isoformat(), txt))

    def __init__(self):
        self.dataclose=self.datas[0].close
        self.MACD = bt.indicators.MACD(self.datas[0])
        self.macd = self.MACD.macd
        self.signal = self.MACD.signal
        self.order = None
        self.buyprice = None
        self.buycomm = None


    # 交易状态检测
    def notify_order(self, order):
        if order.status in [order.Submitted, order.Accepted]:
            return
        if order.status in [order.Completed]:
            if order.isbuy():
                self.log(
                    '买进的价格 %.2f,账户值；%.2f,交易费用：%.2f' % (order.executed.price, order.executed.value, order.executed.comm))
                self.buyprice = order.executed.price
                self.buycomm = order.executed.comm
            if order.issell():
                self.log(
                    '卖出的价格 %.2f,账户值；%.2f,交易费用：%.2f' % (order.executed.price, order.executed.value, order.executed.comm))
        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            self.log('交易取消,资金不足，交易拒接')

    # 交易完统计
    def notify_trade(self, trade):
        if not trade.isclosed:
            return
        self.log('利润：%.2f,总利润: %.2f' % (trade.pnl, trade.pnlcomm))

    # 交易函数
    def next(self):
        # 低位金叉买入
        if self.macd[-1] < self.signal[-1]:
            if self.macd[0] > self.signal[0]:
                if self.macd[0] < 0:
                    self.buy()
                    self.log('MACD低位金叉买入价格:%.2f' % self.dataclose[0])
        # 正常金叉买价
        if self.macd[-1] < self.signal[-1]:
            if self.macd[0] > self.signal[0]:
                if self.macd[0] == 0:
                    self.buy()
                    self.log('MACD正常金叉买入价格:%.2f' % self.dataclose[0])
        # 高位金叉买价，高位金叉有加速上升的作用
        if self.macd[-1] < self.signal[-1]:
            if self.macd[0] > self.signal[0]:
                if self.macd[0] > 0:
                    self.buy()
                    self.log('MACD高位金叉买入价格:%.2f' % self.dataclose[0])
        # 高位死叉卖出
        if self.macd[-1] > self.signal[-1]:
            if self.macd[0] < self.signal[-1]:
                if self.macd[0] >= 0:
                    self.sell()
                    self.log('MACD高位死叉卖出价格:%.2f' % self.dataclose[0])
        # 低位死叉卖出，和死叉减创
        if self.macd[-1] < self.signal[-1]:
            if self.macd[0] > self.signal[0]:
                if self.macd[0] < 0:
                    self.buy()
                    self.log('MACD低位金叉卖出价格:%.2f' % self.dataclose[0])
        # 低位死叉，加速下降卖出
        if self.macd[-1] > self.signal[-1]:
            if self.macd[0] < self.signal[-1]:
                if self.macd[0] < 0:
                    self.sell()
                    self.log('MACD低位死叉卖出价格:%.2f' % self.dataclose[0])


if __name__ == '__main__':
    # 股票池
    stock_list = ['000001',  ]
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
                                   fromdate=datetime.datetime(2013, 1, 1),
                                   todate=datetime.datetime(2022, 12, 31),
                                   )
        cerebro.adddata(data, name=stock)

    client.close()

    # broker设置资金、手续费
    cerebro.broker.setcash(1000000.0)
    cerebro.broker.setcommission(commission=0.001)
    # 设置买入设置，固定买卖数量
    cerebro.addsizer(bt.sizers.FixedSize, stake=1000)

    cerebro.run()
    print('最终的值: %.2f' % cerebro.broker.getvalue())

    b = Bokeh(style='bar', plot_mode='single', scheme=Tradimo())
    cerebro.plot(b)
