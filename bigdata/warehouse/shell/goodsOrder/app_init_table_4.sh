#!/bin/bash
# 需求四：漏斗分析
# app层数据库和表初始化脚本，只需要执行一次即可

hive -e "
create database if not exists app_warehousedb;

create external table if not exists app_warehousedb.app_user_conver_funnel(
    active_num    int,
    item_num     int,
    order_num    int,
    pay_num    int,
    active_to_item_ratio    decimal(10,2),
    item_to_order_ratio    decimal(10,2),
    order_to_pay_ratio    decimal(10,2)
)partitioned by(dt string) 
 row format delimited  
 fields terminated by '\t'
 location 'hdfs://emon:8020/custom/data/warehouse/app/user_conver_funnel/';
"

