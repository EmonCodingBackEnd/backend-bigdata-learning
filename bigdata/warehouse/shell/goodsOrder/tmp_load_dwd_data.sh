#!/bin/bash
# 计算dwd层的数据

for ((i = 1; i <= 28; i++)); do
  if [ $i -lt 10 ]; then
    dt="2026020"$i
  else
    dt="202602"$i
  fi
  echo "dwd_add_partition.sh" ${dt}
  sh /home/emon/bigdata/warehouse/shell/goodsOrder/dwd_add_partition.sh ${dt}
done
