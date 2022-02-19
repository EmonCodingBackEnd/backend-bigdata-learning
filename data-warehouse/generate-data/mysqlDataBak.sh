#!/bin/bash

mysqldump --single-transaction --routines --triggers --events -uflyin -pFlyin@123 -hemon -P3306 --set-gtid-purged=OFF warehousedb > warehousedb.sql