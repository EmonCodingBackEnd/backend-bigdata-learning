#!/bin/bash
# 需求五：操作系统活跃用户相关指标
# app层数据库和表初始化脚本，只需要执行一次即可

hive -e "
create database if not exists app_warehousedb;

create external table if not exists app_warehousedb.app_user_platform_distrib(
    ty    string,
    num    int
)row format delimited  
 fields terminated by '\t'
 location 'hdfs://emon:8020/custom/data/warehouse/app/user_platform_distrib';
 

create external table if not exists app_warehousedb.app_user_android_osver_distrib(
    ty    string,
    num    int
)row format delimited  
 fields terminated by '\t'
 location 'hdfs://emon:8020/custom/data/warehouse/app/user_android_osver_distrib';


create external table if not exists app_warehousedb.app_user_ios_osver_distrib(
    ty    string,
    num    int
)row format delimited  
 fields terminated by '\t'
 location 'hdfs://emon:8020/custom/data/warehouse/app/user_ios_osver_distrib';


create external table if not exists app_warehousedb.app_user_brand_distrib(
    ty    string,
    num    int
)row format delimited  
 fields terminated by '\t'
 location 'hdfs://emon:8020/custom/data/warehouse/app/user_brand_distrib';

create external table if not exists app_warehousedb.app_user_model_distrib(
    ty    string,
    num    int
)row format delimited  
 fields terminated by '\t'
 location 'hdfs://emon:8020/custom/data/warehouse/app/user_model_distrib';



create external table if not exists app_warehousedb.app_user_net_distrib(
    ty    string,
    num    int
)row format delimited  
 fields terminated by '\t'
 location 'hdfs://emon:8020/custom/data/warehouse/app/user_net_distrib';

"

