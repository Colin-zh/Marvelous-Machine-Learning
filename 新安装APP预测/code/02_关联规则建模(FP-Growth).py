# -*- coding: utf-8 -*-

############################ 提取PKG ############################
import datetime

from pyspark.sql.types import StringType, ArrayType
from pyspark.sql import functions as F

# 426597
df = spark.sql("""select gid,pkg_name,create_time,update_time,pkgs from zhengh_yzx.cq_current_install_ranked""")

# 回溯APP安装列表
def pkgs_association(create_time,pkgs,trace_back=None):
    """
    @param:update_time，输入为种子app更新时间
    @param:pkgs 用户app安装列表，用;分隔，格式为pkg1,versionCode,versionName,create_time,update_time,pre_flag;
    @param:trace_back 回溯时长，取15或30天作为回溯的app使用列表
    """
    # 格式化标准日期
    stdd_time = datetime.datetime.strptime(create_time, "%Y-%m-%d %H:%M:%S")
    # 初始化返回列表
    res_lst = []
    if trace_back:
        # 确定回溯日期
        back_time = stdd_time + datetime.timedelta(days=-trace_back)
        # 确定满足回溯时间内的pkg列表
        for pkg in pkgs.split(';'):
            if len(pkg.split(',')) == 6:
                pkg_name,version_code,version_name,pkg_create_time,pkg_update_time,pre_flag = pkg.split(',')
            else:
                continue
            # 剔除预安装APP
            if pre_flag == 1:
                continue
            # 部分时间格式错误：('٢٠١٨-٠٦-١٦ ١١:٠٣:٤٨';'၂၀၁၈-၀၅-၀၂ ၁၄:၁၉:၂၃')
            try:
                pkg_create_time = datetime.datetime.strptime(pkg_create_time, "%Y-%m-%d %H:%M:%S")
                pkg_update_time = datetime.datetime.strptime(pkg_update_time, "%Y-%m-%d %H:%M:%S")
            except Exception as e:
                continue
            if pkg_create_time >= back_time and pkg_create_time <= stdd_time:
                res_lst.append(pkg_name)
                continue
            if pkg_create_time < back_time and pkg_update_time >= back_time:
                res_lst.append(pkg_name)
                continue
    else:
        for pkg in pkgs.split(';'):
            if len(pkg.split(',')) == 6:
                pkg_name,version_code,version_name,pkg_create_time,pkg_update_time,pre_flag = pkg.split(',')
            else:
                continue
            # 剔除预安装APP
            if pre_flag == 1:
                continue
            pkg_create_time = datetime.datetime.strptime(pkg_create_time, "%Y-%m-%d %H:%M:%S")
            if pkg_create_time > stdd_time:
                continue
            res_lst.append(pkg_name)
    # 判断返回pkg列表，若长度为空返回null
    if not res_lst:
        return [""]
    return res_lst

pkgs_association = F.udf(pkgs_association,ArrayType(StringType()))

# 添加回溯15天安装列表信息
df = df.withColumn('pkgs_install_before_15d', pkgs_association("create_time","pkgs",F.lit(15)))

# 添加回溯30天安装列表信息
df = df.withColumn('pkgs_install_before_30d', pkgs_association("create_time","pkgs",F.lit(30)))

# 添加回溯90天安装列表信息
df = df.withColumn('pkgs_install_before_90d', pkgs_association("create_time","pkgs",F.lit(90)))

# 添加回溯安装列表信息
df = df.withColumn('pkgs_install_before_tot', pkgs_association("create_time","pkgs",F.lit(None)))

df = df.drop('pkgs')

df.cache()
df.count()

# 其中4列完全一致的有377255行

# 制表存储
df.registerTempTable('tmpTable')
spark.sql("""create table zhengh_yzx.cq_current_install_associantions as select * from tmpTable""")

############################ 关联规则 ############################
# /opt/spark24/bin/pyspark --master yarn-client --driver-memory 12G --executor-memory 24G  --num-executors 40 --executor-cores 4
from pyspark.sql import functions as F
from pyspark.ml.fpm import FPGrowth

# 重新读取
df = spark.sql("""select pkgs_install_before_30d from zhengh_yzx.cq_current_install_associantions""")

# FP-Growth
def fpgrowth_with_traceback(df = df, itemsCol=None, limit = None , minSupport = 0.01, minConfidence = 0.3):
    # 剔除回溯列表为空的行，经统计后无需作剔除
    # df_cleaned = df.select(itemsCol).where("%s != ''"%itemsCol)
    # df = spark.sql("""select %s from zhengh_yzx.cq_current_install_associantions"""%itemsCol)
    if limit:
        df_cleaned = df.select(itemsCol).filter(F.size(itemsCol)<=limit)
    else:
        df_cleaned = df.select(itemsCol)
    fp = FPGrowth(itemsCol = itemsCol, minSupport=minSupport, minConfidence=minConfidence)
    fpm = fp.fit(df_cleaned)
    return fpm

#### 30d ####
# 初始化关联模型，设置APP数量上限为20
# df_cleaned 111378
# df.select('pkgs_install_before_30d').filter(F.size('pkgs_install_before_30d')<=20).count()
fpm_30d = fpgrowth_with_traceback(df, itemsCol = 'pkgs_install_before_30d', limit = None, minSupport=0.004, minConfidence = 0.01)

# 关联结果频繁项
fpm_30d_freqItemsets = fpm_30d.freqItemsets
# fpm_30d_freqItemsets.orderBy(fpm_30d _freqItemsets.freq.desc()).show(100, truncate=False)
fpm_30d_freqItemsets.cache()
fpm_30d_freqItemsets.count()
fpm_30d_freqItemsets.registerTempTable("cq_current_install_30d_freqItemsets_new")
spark.sql("""create table zhengh_yzx.cq_current_install_30d_freqItemsets_new as select * from cq_current_install_30d_freqItemsets_new order by freq desc""")

# 关联规则筛选
fpm_30d_assRule = fpm_30d.associationRules
fpm_30d_assRule.registerTempTable("cq_current_install_30d_assRules_new")
spark.sql("""drop table if exists zhengh_yzx.cq_current_install_30d_assRules_new""")
spark.sql("""create table zhengh_yzx.cq_current_install_30d_assRules_new as 
             select *
             from cq_current_install_30d_assRules_new
             where array_contains(consequent,'com.tencent.lycqsh')
             or array_contains(consequent,'com.tencent.tmgp.rxcq')
             or array_contains(consequent,'com.tencent.cqsj')
    """)

# 用作关联预测
# new_data = spark.createDataFrame([(["t", "s"], )], ["items"])
# sorted(fpm.transform(new_data).first().prediction)



