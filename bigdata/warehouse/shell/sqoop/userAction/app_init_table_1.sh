#!/bin/bash
# 需求一：每日新增用户相关指标
# app层数据库和表初始化脚本，只需要执行一次即可

hive -e "
create database if not exists app_warehousedb;

create external table if not exists app_warehousedb.app_user_new_count(
    num    int
)partitioned by(dt string)
 row format delimited  
 fields terminated by '\t'
 location 'hdfs://emon:8020/custom/data/warehouse/app/user_new_count';


create external table if not exists app_warehousedb.app_user_new_count_ratio(
    num    int,
    day_ratio    double,
    week_ratio    double
)partitioned by(dt string)
 row format delimited  
 fields terminated by '\t'
 location 'hdfs://emon:8020/custom/data/warehouse/app/user_new_count_ratio';

"

