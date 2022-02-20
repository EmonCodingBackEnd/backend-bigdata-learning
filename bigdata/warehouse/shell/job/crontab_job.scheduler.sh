#!/bin/bash

# 默认获取昨天的日期，也支持传参指定一个日期
if [ "z$1" = "z" ]; then
  dt=$(date +%Y%m%d --date="1 days ago")
else
  dt=$1
fi

# 执行jobA
# 先删除jobA的输出目录
hdfs dfs -rm -r hdfs://emon:8020/custom/data/warehouse/dwd/user_addr/dt=${dt}

hive -e "
insert overwrite table dwd_warehousedb.dwd_user_addr partition(dt='${dt}')  select 
   addr_id,
   user_id,
   addr_name,
   order_flag,
   user_name,
   mobile
from ods_warehousedb.ods_user_addr
where dt = '${dt}' and addr_id is not null;
"

#如果jobA执行成功，这条查询命令就可以成功执行，否则在执行的时候会报错
hdfs dfs -ls hdfs://emon:8020/custom/data/warehouse/dwd/user_addr/dt=${dt}/000000_0

# 在这里通过$?来判断上一条命令是否成功执行，如果$?的返回值为0，则表示jobA执行成功，否则表示jobA执行失败
if [ $? = 0 ]; then
  echo "执行jobB"
else
  # 可以在这里发短息或者发邮件
  echo "jobA执行失败，请处理..."
fi
