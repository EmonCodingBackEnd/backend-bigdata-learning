#!/bin/bash
# ods层数据库和表初始化脚本，只需要执行一次即可

hive -e "
create database if not exists ods_warehousedb;

create external table if not exists ods_warehousedb.ods_user(
   user_id              bigint,
   user_name            string,
   user_gender          tinyint,
   user_birthday        string,
   e_mail               string,
   mobile               string,
   register_time        string,
   is_blacklist         tinyint
)partitioned by(dt string) 
 row format delimited  
 fields terminated by '\t'
 location 'hdfs://emon:8020/custom/data/warehouse/ods/user/';
 
create external table if not exists ods_warehousedb.ods_user_extend(
   user_id              bigint,
   is_pregnant_woman    tinyint,
   is_have_children     tinyint,
   is_have_car          tinyint,
   phone_brand          string,
   phone_cnt            int,
   change_phone_cnt     int,
   weight               int,
   height               int
)partitioned by(dt string) 
 row format delimited  
 fields terminated by '\t'
 location 'hdfs://emon:8020/custom/data/warehouse/ods/user_extend/';
 
create external table if not exists ods_warehousedb.ods_user_addr(
   addr_id              bigint,
   user_id              bigint,
   addr_name            string,
   order_flag           tinyint,
   user_name            string,
   mobile               string
)partitioned by(dt string) 
 row format delimited  
 fields terminated by '\t'
 location 'hdfs://emon:8020/custom/data/warehouse/ods/user_addr/';
 
create external table if not exists ods_warehousedb.ods_goods_info(
   goods_id             bigint,
   goods_no             string,
   goods_name           string,
   curr_price           double,
   third_category_id    int,
   goods_desc           string,
   create_time          string
)partitioned by(dt string) 
 row format delimited  
 fields terminated by '\t'
 location 'hdfs://emon:8020/custom/data/warehouse/ods/goods_info/';
 
create external table if not exists ods_warehousedb.ods_category_code(
   first_category_id    int,
   first_category_name  string,
   second_category_id   int,
   second_catery_name   string,
   third_category_id    int,
   third_category_name  string
)partitioned by(dt string) 
 row format delimited  
 fields terminated by '\t'
 location 'hdfs://emon:8020/custom/data/warehouse/ods/category_code/';

create external table if not exists ods_warehousedb.ods_user_order(
   order_id             bigint,
   order_date           string,
   user_id              bigint,
   order_money          double,
   order_type           int,
   order_status         int,
   pay_id               bigint,
   update_time          string
)partitioned by(dt string) 
 row format delimited  
 fields terminated by '\t'
 location 'hdfs://emon:8020/custom/data/warehouse/ods/user_order/';
 
create external table if not exists ods_warehousedb.ods_order_item(
   order_id             bigint,
   goods_id             bigint,
   goods_amount         int,
   curr_price           double,
   create_time          string
)partitioned by(dt string) 
 row format delimited  
 fields terminated by '\t'
 location 'hdfs://emon:8020/custom/data/warehouse/ods/order_item/';
 
create external table if not exists ods_warehousedb.ods_order_delivery(
   order_id             bigint,
   addr_id              bigint,
   user_id              bigint,
   carriage_money       double,
   create_time          string
)partitioned by(dt string) 
 row format delimited  
 fields terminated by '\t'
 location 'hdfs://emon:8020/custom/data/warehouse/ods/order_delivery/';
 
create external table if not exists ods_warehousedb.ods_payment_flow(
   pay_id               bigint,
   order_id             bigint,
   trade_no             bigint,
   pay_money            double,
   pay_type             int,
   pay_time             string
)partitioned by(dt string) 
 row format delimited  
 fields terminated by '\t'
 location 'hdfs://emon:8020/custom/data/warehouse/ods/payment_flow/';

"

