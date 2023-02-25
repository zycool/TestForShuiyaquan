#  -*- coding: utf-8 -*-
"""
@author:neo
@time:
分析数据时，经常会遇到计算分位数的情况，特此记录一下，以10分位数、50分位数、90分位数为例。
"""
import pandas as pd
import numpy as np
import math

nums = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]


# numpy 计算分位数
# print(np.median(nums))  # 中位数
# print(np.percentile(nums, [50, ]))  # 中位数
# print(np.percentile(nums, [25, 50, 75]))  # 中位数
# print(np.percentile(nums, [10, 50, 90]))  # 10分位数、中位数、90分位数

# pandas 计算分位数
# data = pd.DataFrame({
#     'col1': nums,
# })
# print(data['col1'].quantile(0.5))# 中位数
# print(data['col1'].quantile([0.1, 0.5, 0.9]))# 10分位数、中位数、90分位数
# print(np.percentile(data['col1'], [10, 50, 90]))#这里也可以用numpy计算分位数

# 自定义计算分位数的函数
def percentile(N, percent, key=lambda x: x):
    N = sorted(N)
    k = (len(N) - 1) * percent
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return key(N[int(k)])
    d0 = key(N[int(f)]) * (c - k)
    d1 = key(N[int(c)]) * (k - f)
    return round(d0 + d1, 5)

print(percentile(nums, 0.5))
print(percentile(nums, 0.1))
