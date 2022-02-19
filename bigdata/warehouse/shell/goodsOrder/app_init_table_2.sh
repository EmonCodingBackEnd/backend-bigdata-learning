#!/bin/bash
# 需求二：电商GMV
# app层数据库和表初始化脚本，只需要执行一次即可

hive -e "
create database if not exists app_warehousedb;

create external table if not exists app_warehousedb.app_gmv(
    gmv   decimal(10,2) 
)partitioned by(dt string) 
 row format delimited  
 fields terminated by '\t'
 location 'hdfs://emon:8020/custom/data/warehouse/app/gmv/';
"

