#!/bin/bash
# 给ods层的表添加分区，这个脚本后期每天执行一次
# 每天凌晨，添加昨天的分区，添加完分区之后，再执行后面的计算脚本

# 默认获取昨天的日期，也支持传参指定一个日期
if [ "z$1" = "z" ]; then
  dt=$(date +%Y%m%d --date="1 days ago")
else
  dt=$1
fi

#alter table ods_warehousedb.ods_user add if not exists partition(dt='20260101') location '20260101';
#alter table ods_warehousedb.ods_user_extend add if not exists partition(dt='20260101') location '20260101';
#alter table ods_warehousedb.ods_user_addr add if not exists partition(dt='20260101') location '20260101';
#alter table ods_warehousedb.ods_goods_info add if not exists partition(dt='20260101') location '20260101';
#alter table ods_warehousedb.ods_category_code add if not exists partition(dt='20260101') location '20260101';
#alter table ods_warehousedb.ods_user_order add if not exists partition(dt='20260101') location '20260101';
#alter table ods_warehousedb.ods_order_item add if not exists partition(dt='20260101') location '20260101';
#alter table ods_warehousedb.ods_order_delivery add if not exists partition(dt='20260101') location '20260101';
#alter table ods_warehousedb.ods_payment_flow add if not exists partition(dt='20260101') location '20260101';

sh /home/emon/bigdata/warehouse/shell/goodsOrder/add_partition.sh ods_warehousedb.ods_user ${dt} ${dt}
sh /home/emon/bigdata/warehouse/shell/goodsOrder/add_partition.sh ods_warehousedb.ods_user_extend ${dt} ${dt}
sh /home/emon/bigdata/warehouse/shell/goodsOrder/add_partition.sh ods_warehousedb.ods_user_addr ${dt} ${dt}
sh /home/emon/bigdata/warehouse/shell/goodsOrder/add_partition.sh ods_warehousedb.ods_goods_info ${dt} ${dt}
sh /home/emon/bigdata/warehouse/shell/goodsOrder/add_partition.sh ods_warehousedb.ods_category_code ${dt} ${dt}
sh /home/emon/bigdata/warehouse/shell/goodsOrder/add_partition.sh ods_warehousedb.ods_user_order ${dt} ${dt}
sh /home/emon/bigdata/warehouse/shell/goodsOrder/add_partition.sh ods_warehousedb.ods_order_item ${dt} ${dt}
sh /home/emon/bigdata/warehouse/shell/goodsOrder/add_partition.sh ods_warehousedb.ods_order_delivery ${dt} ${dt}
sh /home/emon/bigdata/warehouse/shell/goodsOrder/add_partition.sh ods_warehousedb.ods_payment_flow ${dt} ${dt}

