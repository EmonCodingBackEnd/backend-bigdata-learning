#!/bin/bash
# 需求六：APP崩溃相关指标
# app层数据库和表初始化脚本，只需要执行一次即可

hive -e "
create database if not exists app_warehousedb;

create external table if not exists app_warehousedb.app_app_close_platform_all(
    ty    string,
	num    int
)partitioned by(dt string)
 row format delimited  
 fields terminated by '\t'
 location 'hdfs://emon:8020/custom/data/warehouse/app/app_close_platform_all';


create external table if not exists app_warehousedb.app_app_close_android_vercode(
    ty    string,
	num    int
)partitioned by(dt string)
 row format delimited  
 fields terminated by '\t'
 location 'hdfs://emon:8020/custom/data/warehouse/app/app_close_android_vercode';


create external table if not exists app_warehousedb.app_app_close_ios_vercode(
    ty    string,
	num    int
)partitioned by(dt string)
 row format delimited  
 fields terminated by '\t'
 location 'hdfs://emon:8020/custom/data/warehouse/app/app_close_ios_vercode';
"
