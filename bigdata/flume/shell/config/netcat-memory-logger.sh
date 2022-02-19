#!/bin/bash
flume_path=/usr/local/flume
nohup flume-ng agent --conf $flume_path/conf --conf-file $flume_path/config/netcat-memory-logger.conf --name a1 -Dflume.root.logger=INFO,LOGFILE &
