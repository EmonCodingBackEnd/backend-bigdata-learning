#!/bin/bash
# 需求四：每日启动APP次数相关指标
# app层数据库和表初始化脚本，只需要执行一次即可

hive -e "
create database if not exists app_warehousedb;

create external table if not exists app_warehousedb.app_user_open_app_count(
    pv    int,
    uv    int
)partitioned by(dt string)
 row format delimited  
 fields terminated by '\t'
 location 'hdfs://emon:8020/custom/data/warehouse/app/user_open_app_count';
 
create external table if not exists app_warehousedb.app_user_open_app_distrib(
    ts_1    int,
    ts_2    int,
    ts_3_m    int
)partitioned by(dt string)
 row format delimited  
 fields terminated by '\t'
 location 'hdfs://emon:8020/custom/data/warehouse/app/user_open_app_distrib';
"

