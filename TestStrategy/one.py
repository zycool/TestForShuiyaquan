#  -*- coding: utf-8 -*-
"""
@author:neo
@time:
"""
import datetime
import pymongo
import backtrader as bt
import pandas as pd
import numpy as np
import math
from backtrader_plotting import Bokeh
from backtrader_plotting.schemes import Tradimo
from CommonKnowledgePoints.Kelly import Kelly


class TestStrategy(bt.Strategy):
    params = (
        ('printlog', True),
        ('log_to_txt', True),
        ('max_stock_value', 200000),  # 单只股票最高开仓市值
        ('buy_vol', dict()),
        ('stop_price', dict())
    )

    def __init__(self):
        self.dataclose = dict()
        self.dataopen = dict()
        self.datalow = dict()
        self.datavolume = dict()
        # To keep track of pending orders and buy price/commission
        self.order = dict()
        self.buyprice = dict()
        self.buycomm = dict()

        for data in self.datas:
            self.dataclose[data._name] = data.close
            self.dataopen[data._name] = data.open
            self.datalow[data._name] = data.low
            self.datavolume[data._name] = data.volume
            self.order[data._name] = None
            self.buyprice[data._name] = None
            self.buycomm[data._name] = None

    def log(self, txt, dt=None, doprint=False):
        if self.params.printlog or doprint:
            dt = dt or self.datas[0].datetime.date(-1)
            # dt = dt or self.datas[0].datetime.date(0)#如果第二天开盘价买，就是这个
            print('%s, %s' % (dt.isoformat(), txt))

    def log_to_txt(self, txt, file_name="xxx.txt"):
        if self.params.log_to_txt:
            dt = self.datas[0].datetime.date(-1)
            f = open(file_name, 'a+')
            txt = str(dt.isoformat()) + " " + txt
            f.writelines(txt)
            f.write('\n')
            f.close()

    # 交易状态检测
    def notify_order(self, order):
        if order.status in [order.Submitted, order.Accepted]:
            return
        file_name = order.data._name + ".txt"
        if order.status in [order.Completed]:
            if order.isbuy():
                txt = '买进 %s 的价格 %.2f,账户值；%.2f,交易费用：%.2f' % (
                    order.data._name, order.executed.price, order.executed.value, order.executed.comm)
                self.log(txt)
                self.log_to_txt(txt, file_name)
                self.buyprice[order.data._name] = order.executed.price
                self.buycomm[order.data._name] = order.executed.comm
            elif order.issell():
                txt = '卖出 %s 的价格 %.2f,账户值；%.2f,交易费用：%.2f' % (
                    order.data._name, order.executed.price, order.executed.value, order.executed.comm)
                self.log(txt)
                self.log_to_txt(txt, file_name)
            self.bar_executed = len(self)
        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            txt = '交易取消,资金不足，交易拒接'
            self.log(txt)
            self.log_to_txt(txt, file_name)

    # 交易完统计
    def notify_trade(self, trade):
        if not trade.isclosed:
            return
        txt = '利润：%.2f,总利润: %.2f' % (trade.pnl, trade.pnlcomm)
        self.log(txt)
        self.log_to_txt(txt, trade.data._name + ".txt")

    def next(self):
        for data in self.datas:
            vol_lines = self.datavolume[data._name]
            pos_size = self.getposition(data).size  # 获取仓位
            if pos_size == 0:
                if vol_lines[0] < vol_lines[-1] * 0.68:  # 当天缩量（小于头天的68%）
                    sum1 = 0  # 大涨前5天的量和
                    sum2 = 0  # 大涨前10到6天的量和
                    for t in range(-6, -1):
                        sum1 += vol_lines[t]
                    for t in range(-11, -6):
                        sum2 += vol_lines[t]
                    if sum1 < sum2 * 2:  # 后5天不能超前5天的两倍
                        close_lines = self.dataclose[data._name]
                        limit = (close_lines[0] - close_lines[-1]) / close_lines[-1]
                        if -0.05 < limit < 0.03:  # 当天的涨幅（-5%,3%）
                            p_limit = (close_lines[-1] - close_lines[-2]) / close_lines[-2]
                            if p_limit > 0.05:  # 头天涨幅大于
                                pos = math.floor(self.params.max_stock_value / close_lines[0])
                                self.order[data._name] = self.buy(data=data, size=pos)
                                self.params.buy_vol[data._name] = vol_lines[0]
                                self.params.stop_price[data._name] = self.datalow[data._name][-1]
            else:  # 有仓，执行卖策略
                if vol_lines[0] > self.params.buy_vol[data._name] * 1.1:
                    # 成交量 大于 买入当天成交量的1.1倍，卖出
                    self.order[data._name] = self.sell(data=data, size=pos_size)
                elif self.dataclose[data._name][0] < self.params.stop_price[data._name]:
                    # 止损
                    self.order[data._name] = self.sell(data=data, size=pos_size)


def print_analyzers(result=None):
    # 常用指标提取
    analyzer = {}
    print("年化收益率: {}%".format(round(result[0].analyzers._Returns.get_analysis()['rnorm100'], 2)))
    print("最大回撤: {}%".format(round(result[0].analyzers._DrawDown.get_analysis()['max']['drawdown'] * (-1), 2)))
    print("年化夏普比率: {}".format(round(result[0].analyzers._SharpeRatio_A.get_analysis()['sharperatio'], 2)))

    # 日度收益率序列
    # ret = pd.Series(result[0].analyzers._TimeReturn.get_analysis())
    # print(ret)

    kelly_data = result[0].analyzers.kelly.get_analysis()  # 获取其中一个分析者的数据时：可通 _name获取
    print("平均盈利：{}".format(round(kelly_data['avgWins'], 2)))
    print("平均亏损：{}".format(round(kelly_data['avgLosses'], 2)))
    print("盈亏比：{}".format(round(kelly_data['winLossRatio'], 2)))
    print("盈利次数：{}".format(round(kelly_data['numberOfWins'], 2)))
    print("亏损次数：{}".format(round(kelly_data['numberOfLosses'], 2)))
    print("总交易次数：{}".format(round(kelly_data['numberOfTrades'], 2)))
    print("胜率：{}%".format(round(kelly_data['winProb'] * 100, 2)))
    print("凯利比率：{}".format(round(kelly_data['kellyRatio'], 2)))


if __name__ == '__main__':
    start_cash = 1000000
    cerebro = bt.Cerebro()
    cerebro.addstrategy(TestStrategy)

    client = pymongo.MongoClient('mongodb://localhost:27017/')
    stock_db = client['stock_day']
    # 股票池
    stock_list = ['000756', '002230', '002336',]
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

    cerebro.broker.set_coc(True)  # 用当日收盘价成交
    # broker设置资金、手续费
    cerebro.broker.setcash(start_cash)
    cerebro.broker.setcommission(commission=0.0015)
    # 设置买入设置，固定买卖数量
    # cerebro.addsizer(bt.sizers.FixedSize, stake=100)

    # 添加分析指标
    cerebro.addanalyzer(Kelly, _name='kelly')  # 添加分析者对象 Kelly为分析者类
    # 返回年初至年末的年度收益率
    cerebro.addanalyzer(bt.analyzers.AnnualReturn, _name='_AnnualReturn')
    # 计算最大回撤相关指标
    cerebro.addanalyzer(bt.analyzers.DrawDown, _name='_DrawDown')
    # 计算年化收益：日度收益
    cerebro.addanalyzer(bt.analyzers.Returns, _name='_Returns', tann=252)
    # 计算年化夏普比率：日度收益
    cerebro.addanalyzer(bt.analyzers.SharpeRatio, _name='_SharpeRatio', timeframe=bt.TimeFrame.Days, annualize=True,
                        riskfreerate=0)  # 计算夏普比率
    cerebro.addanalyzer(bt.analyzers.SharpeRatio_A, _name='_SharpeRatio_A')
    # 返回收益率时序
    cerebro.addanalyzer(bt.analyzers.TimeReturn, _name='_TimeReturn')

    result = cerebro.run()

    print("股票池：", stock_list)
    print('初始资金: %.2f , 最终的资金: %.2f , 收益：%.2f' % (start_cash,
                                                  cerebro.broker.getvalue(),
                                                  cerebro.broker.getvalue() - start_cash))
    print_analyzers(result)

    # b = Bokeh(style='bar', plot_mode='single', scheme=Tradimo())
    # cerebro.plot(b)
