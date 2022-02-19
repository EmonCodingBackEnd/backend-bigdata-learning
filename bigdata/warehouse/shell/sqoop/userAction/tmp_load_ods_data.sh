#!/bin/bash
# 加载ods层的数据

for ((i = 1; i <= 28; i++)); do
  if [ $i -lt 10 ]; then
    dt="2026020"$i
  else
    dt="202602"$i
  fi

  echo "ods_add_partition.sh" ${dt}
  sh /home/emon/bigdata/warehouse/shell/sqoop/userAction/ods_add_partition.sh ${dt}
done

