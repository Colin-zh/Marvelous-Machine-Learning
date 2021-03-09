#################### 确定目标pkg ####################

-- 从pkg量级表中筛选pkg安装量级较高的APP作为预测目标APP
set hive.support.quoted.identifiers=None;
create table zhengh_yzx.new_install_pkg_selection as
select      `^(day)?+.+$`
from        gezhen_intelligent.pkg_history_install_rank
where       day = 20210227
            and app_name rlike '.*传奇.*'
order by    history_install_num desc
limit       100
;

-- 达叔传奇    com.nof.mobile.dscqxsdk     1560474
-- 蓝月传奇    com.tencent.lycqsh          1220066
-- 热血传奇    com.tencent.tmgp.rxcq       1216443
-- 传奇世界    com.tencent.cqsj            758237
-- 传奇世界3D  com.tencent.woool3d         565592
-- 王者传奇    com.ShengHe.wzcq.aligames   231092

-- 最终选择《蓝月传奇》、《热血传奇》、《传奇世界》作为预测APP目标，
-- 1.《达叔传奇》在例如：应用宝、豌豆荚等正规手机应用安装平台已下架，新安装样本量级会收到影响
-- 2.传奇类游戏本身的量级存在严重不足，各APP新安装更少，故结合三个头部APP量级作为预测目标


#################### 确定关联APP ####################
-- 提取种子APP人群安装列表
set hive.exec.compress.output=true;
set mapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec;
create table zhengh_yzx.cq_current_install as
select      a.gid
            ,b.pkg_name
            ,b.create_time
            ,b.update_time
            ,a.is_active
            ,a.pkgs
from        (
            select      gid
                        ,pkgs
                        ,is_active -- 该gid是否活跃
            from        dw_raw_mid.dw_user_applist_gid_parquet
            where       day = 20210228
            ) a
inner join  (
            select      gid
                        ,pkg_name
                        ,create_time
                        ,update_time
            from        dw_raw_mid.dw_user_applist_gid_detail_parquet
            where       day = 20210228
                        and intall_flag = 0
                        and pre_flag = 0
                        and pkg_name in ('com.tencent.lycqsh','com.tencent.tmgp.rxcq','com.tencent.cqsj')
            ) b
on          a.gid = b.gid
;

-- 提取种子人群活跃列表
set hive.exec.compress.output=true;
set mapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec;
create table zhengh_yzx.cq_current_active as
select 		a.gid
			,a.pkg_name
			,a.create_time
			,a.update_time
			,a.is_active
			,b.day_nonsystem_active_pkgs
			,b.nonsystem_pkgs_size
			,b.day
from 		zhengh_yzx.cq_current_install a
inner join	(
			select      gid
						,day_nonsystem_active_pkgs
						,nonsystem_pkgs_size
						,day
			from 		dw_raw_mid.type24_mid_level2
			where 		day >= 20210128 and day <= 20210228
			) b
on 			a.gid = b.gid
;

-- 对于重复gid仅保留最近update_time的记录
set hive.exec.compress.output=true;
set mapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec;
create table zhengh_yzx.cq_current_install_ranked as
select      a.gid
            ,a.pkg_name
            ,a.create_time
            ,a.update_time
            ,a.pkgs
from        (
            select  *
                    ,row_number() over(partition by gid order by update_time desc) as rk
            from    zhengh_yzx.cq_current_install
            ) a
where       a.rk = 1
;



