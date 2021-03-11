#################### 样本取数 ####################
-- 近三个月新安装、召回安装样本数据
create table zhengh_yzx.cq_new_install as
select      create_time
            ,gid
            ,pkg
            ,category
from        dw_raw_mid.dw_user_app_new_install_mid_daily
where       day >= 20201201
            and pkg in ('com.tencent.lycqsh','com.tencent.tmgp.rxcq','com.tencent.cqsj')
;

create table zhengh_yzx.cq_recall_install as
select      create_time
            ,gid
            ,pkg
            ,category
from        dw_raw_mid.dw_user_app_recall_install_mid_daily
where       day >= 20201201
            and pkg in ('com.tencent.lycqsh','com.tencent.tmgp.rxcq','com.tencent.cqsj')
;

create table zhengh_yzx.cq_new_recall_install as
select      create_time
            ,gid
            ,pkg
            ,category
            ,'new' as install_type
from        zhengh_yzx.cq_new_install
union all
select      create_time
            ,gid
            ,pkg
            ,category
            ,'recall' as install_type
from        zhengh_yzx.cq_new_install
;

-- 所有活跃gid的安装列表
set hive.exec.compress.output=true;
set mapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec;
create table zhengh_yzx.cq_tot_current_install as
select      gid
            ,pkgs
            ,is_active -- 该gid是否活跃
from        dw_raw_mid.dw_user_applist_gid_parquet
where       day = 20210228
            and is_active = 1
;















