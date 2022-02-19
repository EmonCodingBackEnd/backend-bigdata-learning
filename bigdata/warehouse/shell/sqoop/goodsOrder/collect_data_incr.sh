#!/bin/bash

# 全量数据采集，每天执行一次

# 默认获取昨天的日期，也支持传参指定一个日期
yesterday=$1
if [ "$yesterday" = "" ]; then
  yesterday=$(date +%Y%m%d --date="1 days ago")
fi

# 转换日期格式，20260101=>2026-01-01
yesterday_new=$(date +%Y-%m-%d --date="${yesterday}")

# SQL语句
# 增量采集SQL
user_order_sql="select order_id,order_date,user_id,order_money,order_type,order_status,pay_id,update_time from user_order where order_date >= '${yesterday_new} 00:00:00' and order_date <= '${yesterday_new} 23:59:59'"
order_item_sql="select order_id,goods_id,goods_amount,curr_price,create_time from order_item where create_time >= '${yesterday_new} 00:00:00' and create_time <= '${yesterday_new} 23:59:59'"
order_delivery_sql="select order_id,addr_id,user_id,carriage_money,create_time from order_delivery where create_time >= '${yesterday_new} 00:00:00' and create_time <= '${yesterday_new} 23:59:59'"
payment_flow_sql="select pay_id,order_id,trade_no,pay_money,pay_type,pay_time from payment_flow where pay_time >= '${yesterday_new} 00:00:00' and pay_time <= '${yesterday_new} 23:59:59'"

# 路径前缀
path_prefix="hdfs://emon:8020/custom/data/warehouse/ods"

# 输出路径
# 将日期字符串中的 -+: 去掉，并且拼接成HDFS的路径
user_order_path=${path_prefix}"/user_order/${yesterday//[-+:]/}"
order_item_path=${path_prefix}"/order_item/${yesterday//[-+:]/}"
order_delivery_path=${path_prefix}"/order_delivery/${yesterday//[-+:]/}"
payment_flow_path=${path_prefix}"/payment_flow/${yesterday//[-+:]/}"

# 采集数据
echo "开始采集..."
echo "采集表：user_order"
sh /home/emon/bigdata/warehouse/shell/sqoop/goodsOrder/sqoop_collect_data_util.sh "${user_order_sql}" "${user_order_path}"
echo "采集表：order_item"
sh /home/emon/bigdata/warehouse/shell/sqoop/goodsOrder/sqoop_collect_data_util.sh "${order_item_sql}" "${order_item_path}"
echo "采集表：order_delivery"
sh /home/emon/bigdata/warehouse/shell/sqoop/goodsOrder/sqoop_collect_data_util.sh "${order_delivery_sql}" "${order_delivery_path}"
echo "采集表：payment_flow"
sh /home/emon/bigdata/warehouse/shell/sqoop/goodsOrder/sqoop_collect_data_util.sh "${payment_flow_sql}" "${payment_flow_path}"

