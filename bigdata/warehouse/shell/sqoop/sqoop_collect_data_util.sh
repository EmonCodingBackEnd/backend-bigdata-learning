#!/bin/bash

# 采集MySQL中的数据导入到HDFS中
if [ $# != 2 ]; then
  echo "参数异常：sqoop_collect_data_util.sh <sql> <hdfs_path>"
  exit 100 # 0表示成功，1-255任意数字表示失败
fi

# 数据SQL
# 例如：select id,name from user where id>1
sql=$1

# 导入到HDFS的路径
hdfs_path=$2

sqoop import \
  --connect jdbc:mysql://emon:3306/warehousedb?serverTimezone=UTC\&useSSL=false \
  --username flyin \
  --password Flyin@123 \
  --target-dir "${hdfs_path}" \
  --delete-target-dir \
  --num-mappers 1 \
  --fields-terminated-by '\t' \
  --null-string '\\N' \
  --null-non-string '\\N' \
  --query "${sql}"' and $CONDITIONS'
