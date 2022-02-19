#!/bin/bash

# 同步 emon 服务器宿主目录下bigdata目录到本地
#echo 'emon123' | sudo -S scp -r emon@emon:~/bigdata .
#scp -r emon@emon:~/bigdata . <<EOF
#emon123
#EOF
# pwd => /c/Job/JobResource/IdeaProjects/Idea2020/backend-bigdata-learning/bigdata
bigdataPath="/c/Job/JobResource/IdeaProjects/Idea2020/backend-bigdata-learning/bigdata"
pwdPath=$(pwd)
if [ "$bigdataPath" = "$pwdPath" ]; then
  echo "删除本地bigdata目录（排查脚本sync.sh）"
  for i in ""$(ls); do
    if [ "$i" != sync.sh ]; then
      rm -rf "$i"
    fi
  done
#  ls|grep -v sync.sh|xargs rm -rf
fi

scp -r emon@emon:~/bigdata ../
