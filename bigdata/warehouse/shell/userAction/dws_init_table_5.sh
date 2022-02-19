#!/bin/bash
# 需求五：操作系统活跃用户相关指标
# dws层数据库和表初始化脚本，只需要执行一次即可

hive -e "
create database if not exists dws_warehousedb;

create external table if not exists dws_warehousedb.dws_user_platform_distrib(
    ty    string,
    num    int
)partitioned by(dt string)
 row format delimited  
 fields terminated by '\t'
 location 'hdfs://emon:8020/custom/data/warehouse/dws/user_platform_distrib';


create external table if not exists dws_warehousedb.dws_user_android_osver_distrib(
    ty    string,
    num    int
)partitioned by(dt string)
 row format delimited  
 fields terminated by '\t'
 location 'hdfs://emon:8020/custom/data/warehouse/dws/user_android_osver_distrib';


create external table if not exists dws_warehousedb.dws_user_ios_osver_distrib(
    ty    string,
    num    int
)partitioned by(dt string)
 row format delimited  
 fields terminated by '\t'
 location 'hdfs://emon:8020/custom/data/warehouse/dws/user_ios_osver_distrib';


create external table if not exists dws_warehousedb.dws_user_brand_distrib(
    ty    string,
    num    int
)partitioned by(dt string)
 row format delimited  
 fields terminated by '\t'
 location 'hdfs://emon:8020/custom/data/warehouse/dws/user_brand_distrib';



create external table if not exists dws_warehousedb.dws_user_model_distrib(
    ty    string,
    num    int
)partitioned by(dt string)
 row format delimited  
 fields terminated by '\t'
 location 'hdfs://emon:8020/custom/data/warehouse/dws/user_model_distrib';



create external table if not exists dws_warehousedb.dws_user_net_distrib(
    ty    string,
    num    int
)partitioned by(dt string)
 row format delimited  
 fields terminated by '\t'
 location 'hdfs://emon:8020/custom/data/warehouse/dws/user_net_distrib';

"

