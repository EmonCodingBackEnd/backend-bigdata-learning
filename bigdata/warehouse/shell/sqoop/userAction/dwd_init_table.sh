#!/bin/bash
# dwd层数据库和表初始化脚本，只需要执行一次即可

hive -e "
create database if not exists dwd_warehousedb;

create external table if not exists dwd_warehousedb.dwd_user_active(
    user_id    bigint,
    xaid    string,
    platform    tinyint,
    ver    string,
    vercode    string,
    net    bigint,
    brand    string,
    model    string,
    display    string,
    osver    string,
    acttime    bigint,
    ad_status    tinyint,
    loading_time    bigint
)partitioned by(dt string) 
 row format delimited  
 fields terminated by '\t'
 location 'hdfs://emon:8020/custom/data/warehouse/dwd/user_active/';




create external table if not exists dwd_warehousedb.dwd_click_good(
    user_id    bigint,
    xaid    string,
    platform    tinyint,
    ver    string,
    vercode    string,
    net    bigint,
    brand    string,
    model    string,
    display    string,
    osver    string,
    acttime    bigint,
    goods_id    bigint,
    location    tinyint
)partitioned by(dt string) 
 row format delimited  
 fields terminated by '\t'
 location 'hdfs://emon:8020/custom/data/warehouse/dwd/click_good/';



create external table if not exists dwd_warehousedb.dwd_good_item(
    user_id    bigint,
    xaid    string,
    platform    tinyint,
    ver    string,
    vercode    string,
    net    bigint,
    brand    string,
    model    string,
    display    string,
    osver    string,
    acttime    bigint,
    goods_id    bigint,
    stay_time    bigint,
    loading_time    bigint
)partitioned by(dt string) 
 row format delimited  
 fields terminated by '\t'
 location 'hdfs://emon:8020/custom/data/warehouse/dwd/good_item/';



create external table if not exists dwd_warehousedb.dwd_good_list(
    user_id    bigint,
    xaid    string,
    platform    tinyint,
    ver    string,
    vercode    string,
    net    bigint,
    brand    string,
    model    string,
    display    string,
    osver    string,
    acttime    bigint,
    loading_time    bigint,
    loading_type    tinyint,
    goods_num    tinyint
)partitioned by(dt string) 
 row format delimited  
 fields terminated by '\t'
 location 'hdfs://emon:8020/custom/data/warehouse/dwd/good_list/';


create external table if not exists dwd_warehousedb.dwd_app_close(
    user_id    bigint,
    xaid    string,
    platform    tinyint,
    ver    string,
    vercode    string,
    net    bigint,
    brand    string,
    model    string,
    display    string,
    osver    string,
    acttime    bigint
)partitioned by(dt string) 
 row format delimited  
 fields terminated by '\t'
 location 'hdfs://emon:8020/custom/data/warehouse/dwd/app_close/';
"

