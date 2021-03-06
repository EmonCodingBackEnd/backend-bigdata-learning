#!/bin/bash
# 需求三：商品相关指标
# app层数据库和表初始化脚本，只需要执行一次即可

hive -e "
create database if not exists app_warehousedb;

create external table if not exists app_warehousedb.app_goods_sales_item(
    goods_name    string,
    first_category_name     string,
    order_total    bigint,
    price_total    decimal(10,2)
)partitioned by(dt string) 
 row format delimited  
 fields terminated by '\t'
 location 'hdfs://emon:8020/custom/data/warehouse/app/goods_sales_item/';


create external table if not exists app_warehousedb.app_category_top10(
    first_category_name     string,
    order_total    bigint
)partitioned by(dt string) 
 row format delimited  
 fields terminated by '\t'
 location 'hdfs://emon:8020/custom/data/warehouse/app/category_top10/';
 
"

