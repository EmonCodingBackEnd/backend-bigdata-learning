#!/bin/bash

flume-ng agent --conf /usr/local/flume/conf \
--conf-file /home/emon/bigdata/warehouse/shell/flume/execMemoryHdfs/exec-memory-hdfs.conf \
--name a1 \
-Dflume.root.logger=INFO,console
