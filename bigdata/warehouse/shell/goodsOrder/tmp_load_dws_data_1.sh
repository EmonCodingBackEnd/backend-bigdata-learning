#!/bin/bash
# 计算dwd层的数据

for ((i = 1; i <= 28; i++)); do
  if [ $i -lt 10 ]; then
    dt="2026020"$i
  else
    dt="202602"$i
  fi
  echo "dws_add_partition_1.sh" ${dt}
  sh /home/emon/bigdata/warehouse/shell/goodsOrder/dws_add_partition_1.sh ${dt}
done

