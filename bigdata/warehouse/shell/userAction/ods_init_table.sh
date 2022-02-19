#!/bin/bash

#注意：
#1：建议在创建和使用表的时候，在表名前面加上数据库的名称
#数据库名称.表名称
#
#2：考虑到SQL重跑的情， 需要在SQL语句中添加if not exists
#
#3：string、date、timestamp，建议使用string对日期格式进行统一

# ods层数据库和表初始化脚本，只需要执行一次

hive -e "
create database if not exists ods_warehousedb;

create external table if not exists ods_warehousedb.ods_user_active(
    log    string
)partitioned by (dt string)
 row format delimited
 fields terminated by '\t'
 location 'hdfs://emon:8020/custom/data/warehouse/ods/user_action/';

 
create external table if not exists ods_warehousedb.ods_click_good(
    log    string
)partitioned by (dt string)
 row format delimited
 fields terminated by '\t'
 location 'hdfs://emon:8020/custom/data/warehouse/ods/user_action/';
 
 
create external table if not exists ods_warehousedb.ods_good_item(
    log    string
)partitioned by (dt string)
 row format delimited
 fields terminated by '\t'
 location 'hdfs://emon:8020/custom/data/warehouse/ods/user_action/';

 
create external table if not exists ods_warehousedb.ods_good_list(
    log    string
)partitioned by (dt string)
 row format delimited
 fields terminated by '\t'
 location 'hdfs://emon:8020/custom/data/warehouse/ods/user_action/';


create external table if not exists ods_warehousedb.ods_app_close(
    log    string
)partitioned by (dt string)
 row format delimited
 fields terminated by '\t'
 location 'hdfs://emon:8020/custom/data/warehouse/ods/user_action/';

"
