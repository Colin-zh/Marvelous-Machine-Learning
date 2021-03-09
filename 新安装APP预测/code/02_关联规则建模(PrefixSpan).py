# -*- coding: utf-8 -*-

############################ 基于安装 ############################
import datetime

from pyspark.sql.types import StringType, ArrayType
from pyspark.sql import functions as F

# 426597
df = spark.sql("""select gid,pkg_name,create_time,update_time,pkgs from zhengh_yzx.cq_current_install_ranked""")

# 嵌套列表按指定索引排序
def sortFunc(data_list,index=1):
    '''
    基于内置函数 sorted  实现排序
    '''
    data_list = sorted(data_list,key=lambda i:i[index])
    return data_list

# 若嵌套列表中日期一样，则合并到一起
def groupByDate(data_list):
    '''
    通过判断与上一个日期是否一致
    '''
    res = []
    for idx in range(1,len(data_list)):
        cur_pkg,cur_time = data_list[idx]
        pre_pkg,pre_time = data_list[idx-1]
        if cur_time[:10] == pre_time[:10]:
            if len(res) != 0:
                res[-1].append(cur_pkg)
            else:
                res.append([pre_pkg,cur_pkg])
        else:
            if len(res) != 0:
                res.append([cur_pkg])
            else:
                res = [[pre_pkg],[cur_pkg]]
    return res

# 回溯APP安装列表
def pkgsAssociation(create_time,pkgs,trace_back=None):
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
            # 仅按安装时间
            try:
                pkg_time = datetime.datetime.strptime(pkg_create_time, "%Y-%m-%d %H:%M:%S")
            except Exception as e:
                continue
            if pkg_time < back_time or pkg_time > stdd_time:
                continue
            res_lst.append([pkg_name,pkg_create_time])
    else:
        for pkg in pkgs.split(';'):
            if len(pkg.split(',')) == 6:
                pkg_name,version_code,version_name,pkg_create_time,pkg_update_time,pre_flag = pkg.split(',')
            else:
                continue
            # 剔除预安装APP
            if pre_flag == 1:
                continue
            pkg_time = datetime.datetime.strptime(pkg_create_time, "%Y-%m-%d %H:%M:%S")
            if pkg_time > stdd_time:
                continue
            res_lst.append([pkg_name,pkg_create_time])
    # 判断返回pkg列表，若长度为空返回null
    if not res_lst:
        return [""]
    return groupByDate(sortFunc(res_lst,index=1))

pkgsAssociation = F.udf(pkgsAssociation,ArrayType(ArrayType(StringType())))

# 添加回溯30天安装列表信息
df = df.withColumn('pkgs_install_before_30d', pkgsAssociation("create_time","pkgs",F.lit(30)))

# 制表存储
df.registerTempTable('tmpTable')
spark.sql("""create table zhengh_yzx.cq_current_install_associantions_prefixspan as select * from tmpTable""")

############################ 关联规则 ############################
from pyspark.sql import functions as F
from pyspark.ml.fpm import PrefixSpan

# 重新读取
df = spark.sql("""select pkgs_install_before_30d from zhengh_yzx.cq_current_install_associantions_prefixspan""")

# prefixSpan算法
'''
This class is not yet an Estimator/Transformer, 
use findFrequentSequentialPatterns() method to run the PrefixSpan algorithm.
'''
prefixSpan = PrefixSpan(minSupport=0.001, maxPatternLength=10,sequenceCol='pkgs_install_before_30d')
prefixSpanModel = prefixSpan.findFrequentSequentialPatterns(df)
prefixSpanModel.cache()
prefixSpanModel.sort(prefixSpanModel.freq.desc()).show()
prefixSpanModel.sort(F.size(prefixSpanModel).desc()).show()

############################ 基于活跃 ############################
import datetime

from pyspark.sql.types import StringType, ArrayType
from pyspark.sql import functions as F

# 1713621
df = spark.sql("""select gid,pkg_name,create_time,update_time,day_nonsystem_active_pkgs,day from zhengh_yzx.cq_current_active""")

# 转换
def active_pkgs_transform(day_nonsystem_active_pkgs):
    res = []
    for pkg_info in day_nonsystem_active_pkgs.split(';'):
        res.append(pkg_info.split(',')[0])
    return res

active_pkgs_transform = F.udf(active_pkgs_transform,ArrayType(StringType()))

df = df.withColumn('active_pkgs_list', active_pkgs_transform('day_nonsystem_active_pkgs'))
df = df.drop('day_nonsystem_active_pkgs')

df = df.groupBy('gid').agg(F.sort_array(
                                F.collect_list(
                                    F.concat_ws(':',F.col('day'),F.col('active_pkgs_list').cast(StringType())))).alias('pkgs_with_day'))

def get_active_pkgs(pkgs_with_day):
    res = []
    for pkgs_info in pkgs_with_day:
        pkgs = pkgs_info.split(':')[-1]
        pkgs = pkgs.replace('[','').replace(']','').split(',')
        res.append(pkgs)
    return res

get_active_pkgs = F.udf(get_active_pkgs,ArrayType(ArrayType(StringType())))
df = df.withColumn('active_pkgs_array', get_active_pkgs('pkgs_with_day'))
df.drop('pkgs_with_day')

# 制表存储
# 172693
df.registerTempTable('tmpTable')
spark.sql("""create table zhengh_yzx.cq_current_active_associantions_prefixspan as select * from tmpTable""")

############################ 关联规则 ############################
from pyspark.sql import functions as F
from pyspark.ml.fpm import PrefixSpan

# 重新读取
df = spark.sql("""select active_pkgs_array from zhengh_yzx.cq_current_active_associantions_prefixspan""")

# prefixSpan算法
prefixSpan = PrefixSpan(minSupport=0.003, maxPatternLength=10,sequenceCol='active_pkgs_array')
prefixSpanModel = prefixSpan.findFrequentSequentialPatterns(df)
prefixSpanModel.cache()
prefixSpanModel.sort(prefixSpanModel.freq.desc()).show()
prefixSpanModel.sort(F.size(prefixSpanModel).desc()).show()

# 关联规则过滤
res = prefixSpanModel.filter(
                F.array_contains(F.col('sequence').getItem(F.size(F.col('sequence'))-1),'com.tencent.lycqsh')
                | F.array_contains(F.col('sequence').getItem(F.size(F.col('sequence'))-1),'com.tencent.tmgp.rxcq')
                | F.array_contains(F.col('sequence').getItem(F.size(F.col('sequence'))-1),'com.tencent.cqsj')
                )
res.registerTempTable('tmpTable')
spark.sql("""create table zhengh_yzx.cq_current_active_prefixspan_model as select * from tmpTable""")
