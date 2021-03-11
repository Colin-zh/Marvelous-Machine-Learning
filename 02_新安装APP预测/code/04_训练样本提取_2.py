# -*- coding: utf-8 -*-
import datetime
from collections import defaultdict
from pyspark.sql.types import StringType, ArrayType
from pyspark.sql import functions as F

# 获取关联规则可选列表
association_lst = list(spark.sql("""select antecedent from zhengh_yzx.cq_current_install_final_assrules""")\
                  .distinct().collect())

association_dct = defaultdict(set)
for association in association_lst:
    association_dct[len(i)].add(tuple(association))




# 获取全量当前APP安装列表
# 223592872
df = spark.sql("""select      gid
                              ,pkgs
                  from        dw_raw_mid.dw_user_applist_gid_parquet
                  where       day = 20210228 and is_active = 1""")

# 选取符合关联规则的目标人群
def pkg_selection(pkgs, association_lst):
    # 初始化时间窗口长度
    time_window = 30
    # 初始化目标APP
    target_pkg = ['com.tencent.lycqsh','com.tencent.tmgp.rxcq','com.tencent.cqsj']
    # 初始化返回结果列表
    res_lst = []
    # 遍历pkgs列表
    for pkg in pkgs.split(';'):
        if len(pkg.split(',')) != 6:
            continue
        pkg_name,version_code,version_name,pkg_create_time,pkg_update_time,pre_flag = pkg.split(',')
        # 剔除预安装APP
        if pre_flag == 1:
            continue

