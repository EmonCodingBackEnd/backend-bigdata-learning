-- -------------------------------------------------
-- 不会被SpringBoot调用，项目初始化时通过命令行创建
-- 建库脚本
-- -------------------------------------------------

-- 创建用户
CREATE USER 'flyin'@'%' identified BY 'Flyin@123';
-- 授权用户
GRANT ALL PRIVILEGES ON *.* TO 'flyin'@'%' WITH GRANT OPTION;

-- 创建备份用户
create user 'backup'@'%' identified by 'XXX';
-- 授权备份用户
grant select,reload,lock tables,replication slave,replication client,show view,event,process ON *.* TO 'backup'@'%' with grant option;

-- 创建数据库
CREATE DATABASE IF NOT EXISTS warehousedb DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- 使用数据库
use warehousedb;
