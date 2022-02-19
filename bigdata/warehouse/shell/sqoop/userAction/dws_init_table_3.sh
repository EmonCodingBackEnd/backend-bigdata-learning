#!/bin/bash
# 需求三：用户7日流失push提醒
# dws层数据库和表初始化脚本，只需要执行一次即可

hive -e "
create database if not exists dws_warehousedb;

create external table if not exists dws_warehousedb.dws_user_lost_item(
    xaid    string
)partitioned by(dt string)
 row format delimited  
 fields terminated by '\t'
 location 'hdfs://emon:8020/custom/data/warehouse/dws/user_lost_item';
"

