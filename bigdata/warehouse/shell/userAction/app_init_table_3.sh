#!/bin/bash
# 需求三：用户7日流失push提醒
# app层数据库和表初始化脚本，只需要执行一次即可

hive -e "
create database if not exists app_warehousedb;

create external table if not exists app_warehousedb.app_user_lost_count(
    num    int
)partitioned by(dt string)
 row format delimited  
 fields terminated by '\t'
 location 'hdfs://emon:8020/custom/data/warehouse/app/user_lost_count';
"

