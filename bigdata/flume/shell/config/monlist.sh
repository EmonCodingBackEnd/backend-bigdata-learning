#!/bin/bash
monlist=`cat /usr/local/flume/config/monlist.conf`
echo "start check"
for item in ${monlist}
do
    # 设置字段分隔符
    OLD_IFS=$IFS
    # 以=分隔，转换为数组
    IFS="="
    # 把一行内容转成多列
    arr=(${item})
    # 获取等号左边内容
    name=${arr[0]}
    # 获取等号右边内容
    script=${arr[1]}
    echo "time is:"`date +"%Y-%m-%d %H:%M:%S"`" check "$name
    if [ `jps -m|grep $name | wc -l` -eq 0 ]; then
        # 发短信或者邮件告警
        echo `date +"%Y-%m-%d %H:%M:%S"` $name "is none"
        sh -x ${script}
    else
        echo "time is:"`date +"%Y-%m-%d %H:%M:%S"`" check "$name" is ok!"
    fi
done
