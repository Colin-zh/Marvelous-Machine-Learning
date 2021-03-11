# -*- coding: utf-8 -*-

########### 导入工具包 ###########
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
from datetime import date
import datetime as dt
from scipy import stats
import warnings
warnings.filterwarnings("ignore")

# 修改pandas默认的现实设置
pd.set_option("display.max_columns",10)  
pd.set_option("display.max_rows",20)  

# %matplotlib inline

########### 读取数据 ############
current_install = pd.read_csv("../data/current_install.csv", keep_default_na=False)
current_install.columns = [""]

new_recall_install = pd.read_csv("../data/new_recall_install.csv", keep_default_na=False)

########### 初步探查 ############
current_install.head()
current_install.info()
current_install.describe()
current_install.isnull().sum()

########### 初步分析 ############
# 1 查看日期范围
print(current_install[current_install["day"] != "null"]["day"].min())
print(current_install[current_install["day"] != "null"]["day"].max())
# 2 相关性分析（召当前安装与新安装人群的重合度）
current_install_gid = current_install[['gid']].copy().drop_duplicates()
new_recall_install_gid = new_recall_install[['gid']].copy().drop_duplicates()
print("当前安装人数为%s"%current_install_gid.gid.count())
print("新安装人数为%s"%new_recall_install_gid.gid.count())

new_recall_install_gid['new_recall_install'] = 1
install_merge = current_install_gid.merge(new_recall_install_gid, on='gid',how="left").reset_index().fillna(0)
print("新安装人数与当前安装人数重复为%s"%install_merge[new_recall_install].sum())
# 3 数据分布
# 类别型变量
current_install["day"].value_counts()
plt.rcParams['figure.figsize'] = (25.0, 4.0)                       # 设置图片大小
plt.title("Value Distribution", fontsize=24)                       # 指定标题，并设置标题字体大小
plt.xlabel("Values", fontsize=14)                                  # 指定X坐标轴的标签，并设置标签字体大小
plt.ylabel("Count", fontsize=14)                                   # 指定Y坐标轴的标签，并设置标签字体大小
plt.tick_params(axis='both', labelsize=14)                         # 参数axis值为both，代表要设置横纵的刻度标记，标记大小为14
plt.xticks(size='small', rotation=68,fontsize=8)                   # 设置x轴标签文字的大小(size),倾斜角度(rotation),字体大小(fontsize)
plt.plot(current_install["day"].value_counts(), linewidth=2)
plt.show()                                                         # 打开matplotlib查看器，并显示绘制的图形
# 数值型变量
fig = plt.figure(figsize=(4, 6))  # 指定绘图对象宽度和高度
sns.boxplot(current_install['con_var'],
            orient="v",
            width=0.5)
# 直方图与Q-Q图
plt.figure(figsize=(10, 5))
ax = plt.subplot(1, 2, 1)
sns.distplot(current_install['con_var'],
             fit=stats.norm)
ax = plt.subplot(1, 2, 2)
res = stats.probplot(current_install['con_var'],
                     plot=plt)
# 分布对比
plt.rcParams['figure.figsize'] = (6.0, 4.0)  #设置图片大小
ax = sns.kdeplot(current_install['con_var'],
                 color="Red",
                 shade=True)
ax = sns.kdeplot(new_recall_install['con_var'],
                 color="Blue",
                 shade=True)
ax.set_xlabel('con_var')
ax.set_ylabel("Frequency")
ax = ax.legend(["current", "new_recall"])
# 可视化线性关系
fcols = 2
frows = 1
plt.figure(figsize=(8, 4))
ax = plt.subplot(1, 2, 1)
sns.regplot(x='distance',
            y='label',
            data=current_install['con_var','label'],
            ax=ax,
            scatter_kws={
                'marker': '.',
                's': 3,
                'alpha': 0.3
            },
            line_kws={'color': 'k'})
plt.xlabel('con_var')
plt.ylabel('label')
ax = plt.subplot(1, 2, 2)
sns.distplot(current_install['con_var'])
plt.xlabel('con_var')
plt.show()