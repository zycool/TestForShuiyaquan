#  -*- coding: utf-8 -*-
"""
@author:neo
@time:

关于凯利公式
kelly公式得到的凯利比率，实际上并不是对策略的绩效的评价，那么它起什么作用呢：
如果凯利比率为负，则说明策略是亏损的，不能采用。如果kelly比率（kellyRatio）为正数，比如kellyRatio=0.215
，那么说明，理论上每次下单时，购买金额应该为当时总现金值的 kellyPercent 即 21.5%。
"""

from backtrader import Analyzer
from backtrader.mathsupport import average
from backtrader.utils import AutoOrderedDict
# from logger import lg


class Kelly(Analyzer):

    def create_analysis(self):
        self.rets = AutoOrderedDict()

    def start(self):
        super().start()
        self.pnlWins = list()
        self.pnlLosses = list()

    def notify_trade(self, trade):
        # lg.info(trade.status)
        if trade.status == trade.Closed:
            if trade.pnlcomm > 0:
                # 盈利加入盈利列表，利润0算盈利
                self.pnlWins.append(trade.pnlcomm)
            else:
                # 亏损加入亏损列表
                self.pnlLosses.append(trade.pnlcomm)

    def stop(self):
        kellyPercent = None  # 信息不足
        avgWins = 0
        avgLosses = 0
        winLossRatio = 0
        numberOfWins = 0
        numberOfLosses = 0
        numberOfTrades = 0
        winProb = 0
        if len(self.pnlWins) > 0 and len(self.pnlLosses) > 0:
            avgWins = average(self.pnlWins)  # 计算平均盈利
            avgLosses = abs(average(self.pnlLosses))  # 计算平均亏损（绝对值）
            winLossRatio = avgWins / avgLosses  # 盈亏比
            if winLossRatio == 0:
                kellyPercent = None
            else:
                numberOfWins = len(self.pnlWins)  # 获胜次数
                numberOfLosses = len(self.pnlLosses)  # 亏损次数
                numberOfTrades = numberOfWins + numberOfLosses  # 总交易次数
                winProb = numberOfWins / numberOfTrades  # 计算胜率
                inverse_winProb = 1 - winProb

                # 计算凯利比率，即每次交易投入资金占总资金 占 总资金 的最优比率
                kellyPercent = winProb - (inverse_winProb / winLossRatio)

        self.rets.avgWins=avgWins# 平均盈利
        self.rets.avgLosses=0-avgLosses #平均亏损
        self.rets.winLossRatio=winLossRatio#盈亏比
        self.rets.numberOfWins=numberOfWins# 获胜次数
        self.rets.numberOfLosses = numberOfLosses  # 亏损次数
        self.rets.numberOfTrades = numberOfTrades  # 总交易次数
        self.rets.winProb = winProb  # 胜率

        self.rets.kellyRatio = kellyPercent  # 如：0.215
