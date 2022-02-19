# 一、综合项目：数据仓库

## 1.1、分析

### 1.1.1、数据仓库分层设计

- APP（又称ADS：Application Data Service）数据应用层：为统计报表提供数据
- DWS（Data Warehouse Service）数据汇总层：对数据进行轻度汇总（宽表）
- DWD（Data Warehouse Details）明细数据层：清洗之后的数据
- ODS（Operation Data Store）原始数据层：存放原始数据

### 1.1.2、典型的数据仓库系统架构

![image-20220212150226804](images/image-20220212150226804.png)



### 1.1.3、项目需求分析

想要开发一个完整的数据仓库系统，至少需要以下这几个功能模块：

- 数据采集平台，这个模块主要负责采集各种数据源的数据
- 数据仓库，这个模块负责数据仓库和管理
- 数据报表，这个模块其实就是数据可视化展示了

通过这三个模块可以实现数据采集，构建数据仓库，最后基于数据仓库中的数据实现上层应用，体现数据仓库的价值。

### 1.1.4、数据选项

- 数据采集：Flume【推荐】、Logstash、FileBeat、Sqoop【推荐】
  - Flume：日志采集
  - Sqoop：关系型数据库采集
- 数据存储：HDFS【推荐】、MySQL
- 数据计算：Hive【优先】、Spark
- 数据可视化：Hue、Zeppelin【推荐】、Echarts（开发数据接口）

### 1.1.5、整体架构设计

![image-20220212153256966](images/image-20220212153256966.png)



### 1.1.6、服务器资源规划-测试环境

![image-20220212153551565](images/image-20220212153551565.png)

### 1.1.7、服务器资源规划-生产环境



![image-20220212153657188](images/image-20220212153657188.png)



# 二、电商数据仓库之用户行为数仓

## 1.1、数据采集

### 1.1.1、创建数据库和表

- 创建数据库

```mysql
-- 创建用户
CREATE USER 'flyin'@'%' identified BY 'Flyin@123';
-- 授权用户
GRANT ALL PRIVILEGES ON *.* TO 'flyin'@'%' WITH GRANT OPTION;
-- 创建数据库
CREATE DATABASE IF NOT EXISTS warehousedb DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
-- 使用数据库
use warehousedb;
```

- 创建表

```bash
mysql -uflyin -pFlyin@123 warehousedb < /home/emon/bigdata/warehouse/data/init_mysql_tables.sql
```

### 1.1.2、初始化数据

- 初始化UserAction数据

执行类：`com.coding.bigdata.useraction.GenerateUserActionData`

- 初始化GoodsOrder数据

执行类：`com.coding.bigdata.useraction.GenerateGoodsOrderData`

### 1.1.3、采集数据

- 对小表，全量采集
- 对大表，增量采集（按照变更时间）

| 表名          | 导入方式 | 表名           | 导入方式 |
| ------------- | -------- | -------------- | -------- |
| user          | 全量     | user_order     | 增量     |
| user_extend   | 全量     | order_item     | 增量     |
| user_addr     | 全量     | order_delivery | 增量     |
| goods_info    | 全量     | payment_flow   | 增量     |
| category_code | 全量     |                |          |

#### 1.1.3.1、采集UserAction数据

- 启动flume脚本

```bash
# 先执行该脚本命令，再执行`com.coding.bigdata.useraction.GenerateUserActionData`类产生日志
flume-ng agent --conf /usr/local/flume/conf \
--conf-file /home/emon/bigdata/warehouse/shell/flume/execMemoryHdfs/exec-memory-hdfs.conf \
--name a1 \
-Dflume.root.logger=INFO,console
```

#### 1.1.3.2、采集GoodsOrder数据

- 执行全量采集脚本

```bash
sh /home/emon/bigdata/warehouse/shell/sqoop/goodsOrder/collect_data_full.sh 20260101
```

- 执行增量采集脚本

```bash
sh /home/emon/bigdata/warehouse/shell/sqoop/goodsOrder/collect_data_incr.sh 20260101
```

## 1.2、创建ODS层

- 表介绍

| 表名            | 解释                  |
| --------------- | --------------------- |
| ods_user_active | 用户主动活跃表(act=1) |
| ods_click_good  | 点击商品表(act=2)     |
| ods_good_item   | 商品详情页表(act=3)   |
| ods_good_list   | 商品列表页表(act=4)   |
| ods_app_close   | APP崩溃数据表（act=5  |

- 创建hive库、hive外部表并设置分区

```bash
# 初始化ods库与表
[emon@emon ~]$ sh /home/emon/bigdata/warehouse/shell/sqoop/userAction/ods_init_table.sh 
# 添加分区
[emon@emon ~]$ sh /home/emon/bigdata/warehouse/shell/sqoop/userAction/ods_add_partition.sh 20260101
```

## 1.3、创建DWD层

- 表介绍

| 表名            | 解释                  |
| --------------- | --------------------- |
| dwd_user_active | 用户主动活跃表(act=1) |
| dwd_click_good  | 点击商品表(act=2)     |
| dwd_good_item   | 商品详情页表(act=3)   |
| dwd_good_list   | 商品列表页表(act=4)   |
| dwd_app_close   | APP崩溃数据表（act=5  |

- 创建hive库、hive外部表并设置分区

```bash
# 初始化ods库与表
[emon@emon ~]$ sh /home/emon/bigdata/warehouse/shell/sqoop/userAction/dwd_init_table.sh
# 添加分区
[emon@emon ~]$ sh /home/emon/bigdata/warehouse/shell/sqoop/userAction/dwd_add_partition.sh 20260101
```

## 1.4、需求分析与模拟数据初始化

### 1.4.1、需求列表

- 需求1：每日新增用户相关指标
- 需求2：每日活跃用户（主活）相关指标
- 需求3：用户7日流失push提醒相关指标
- 需求4：每日启动App次数相关指标
- 需求5：操作系统活跃用户相关指标
- 需求6：APP崩溃相关指标

### 1.4.2、模拟20260201-20260228数据

- 初始化UserAction数据

执行类：`com.coding.bigdata.useraction.GenerateUserActionData2`

- 初始化GoodsOrder数据

执行类：`com.coding.bigdata.useraction.GenerateGoodsOrderData2`

### 1.4.3·添加分区

```bash
# 添加ods分区：20260201-20260228
[emon@emon ~]$ sh /home/emon/bigdata/warehouse/shell/sqoop/userAction/tmp_load_ods_data.sh
# 添加dwd分区：20260201-20260228
[emon@emon ~]$ sh /home/emon/bigdata/warehouse/shell/sqoop/userAction/tmp_load_dwd_data.sh
```

## 1.5、需求1：每日新增用户相关指标

新增用户：也指新增设备，指第一次安装并且使用app的用户，后期卸载之后再使用就不是新用户了。

### 1.5.1、指标1：每日新增用户量

- ods层表名：ods_user_active
- dwd层表名：dwd_user_active

第一步：我们基于清洗之后打开app上报的数据创建一个历史表，这个表里面包含的有xaid字段，针对每天的数据基于axid进行去重。

第二步：如果我们要计算2026年2月1日的新增用户量，就拿这一天上报的打开app的数据，和前面的历史表进行left join，使用xaid进行关联，关联不上的数据则为新增数据。

> 举个例子：第一步会产生一个历史表，`dws_user_active_history`，这个表中有一个xaid字段
>
> dws_user_active_history
>
> xaid
>
> a1
>
> b1
>
> c1
>
> d1
>
> 第二步会产生一个临时表，表里面包含的是那一天上报的打开app的数据
>
> dws_user_active_20260201_tmp
>
> xaid
>
> a1
>
> b1
>
> x1
>
> y1
>
> z1
>
> 对这两个表进行left join
>
> dws_user_active_20260201_tmp								dws_user_active_history
>
> xaid																					xaid
>
> a1																						a1
>
> b1																						b1
>
> x1																						null
>
> y1																						null
>
> z1																						null
>
> 此时：`dws_user_active_history.xaid`为null的数据记录数即为当日新增用户数

第三步：将计算出来的每日新增用户信息保存到表`dws_user_new_item`表中，这个表按照天作为分区，便于后期其他需求使用这个表。

第四步：基于`dws_user_new_item`对数据进行聚合，将计算出来的新增用户数量保存到结果表`app_user_new_count`表。

注意：在这里处理完之后，还需要将`dws_user_active_20260201_tmp`这个临时表数据insert到`dws_user_active_history`这个历史表中。

最后，删除这个临时表。

### 1.5.2、指标2：每日新增用户量的日环比和周同比

同比：一般是指本期统计数据和往年的同时期的统计数据比较。

环比：一般是指本期统计数据和上一期的统计数据作比较。



日环比=（本期的数据-上一期的数据）/上一期的数据，日环比中的单位是天。

周同比=（本期的数据-上一期的数据）/上一期的数据，周同比中的单位是周（7天）。



实现思路：

直接基于`app_user_new_count`进行统计即可，可以统计出来某一天的日环比和周同比，生成一个新表`app_user_new_count_ratio`。

里面包含日期、新增用户量、日环比、周同比。

### 1.5.3、汇总总结

我们最终要在DWS创建3个表：

- dws_user_active_20260201_tmp
- dws_user_active_history
- dws_user_new_item

在APP层要创建2个表：

- app_user_new_count
- app_user_new_count_ratio

### 1.5.4、脚本执行之DWS层

针对dws层抽取脚本：

1：表初始化脚本（初始化执行一次）

```bash
# 初始化ods库与表
[emon@emon ~]$ sh /home/emon/bigdata/warehouse/shell/sqoop/userAction/dws_init_table_1.sh
```

2：添加分区数据脚本（每天执行一次）

```bash
# 添加分区：20260201-20260209
[emon@emon ~]$ sh /home/emon/bigdata/warehouse/shell/sqoop/userAction/tmp_load_dws_data_1.sh
```

### 1.5.5、脚本执行之APP层

针对dws层抽取脚本：

1：表初始化脚本（初始化执行一次）

```bash
# 初始化ods库与表
[emon@emon ~]$ sh /home/emon/bigdata/warehouse/shell/sqoop/userAction/app_init_table_1.sh
```

2：添加分区数据脚本（每天执行一次）

```bash
# 添加分区：20260201-20260209
[emon@emon ~]$ sh /home/emon/bigdata/warehouse/shell/sqoop/userAction/tmp_load_app_data_1.sh
```

## 1.6、需求2：每日活跃用户（主活）相关指标

### 1.6.1、指标1：每日主活用户量

直接使用dws层的`dws_user_active_history`这个表，直接求和即可获取到当日的主活用户量，将最终的结果保存到app层的`app_user_active_count`表中。

### 1.6.2、指标2：每日主活用户量的日环比和周同比

这个指标直接基于每日主活用户量的表`app_user_active_count`进行计算即可，把最终的结果保存到app层的`app_user_active_count_ratio`表中。

### 1.6.3、脚本执行之APP层

1：表初始化脚本（初始化执行一次）

```bash
# 初始化ods库与表
[emon@emon ~]$ sh /home/emon/bigdata/warehouse/shell/sqoop/userAction/app_init_table_2.sh
```

2：添加分区数据脚本（每天执行一次）

```bash
# 添加分区：20260201-20260209
[emon@emon ~]$ sh /home/emon/bigdata/warehouse/shell/sqoop/userAction/tmp_load_app_data_2.sh
```

### 1.6.4、扩展需求：如何统计每周每月的主活用户量

每周：按照自然周，每周一凌晨计算上一周的主活。

每月：按照自然月，每月1号计算上一个月的主活。



## 1.7、需求3：用户7日流失push提醒相关指标

如果2.2日首次进入，一直到2.9日都没再登录，算7日流失。

### 1.7.1、指标1：用户7日流失push

第一步：基于 *dws_user_active_history* 表，获取表中最近8天（登陆日+后续7日）的数据，根据xaid进行分组，这样可以获取xaid以及xaid对应的多个日期(dt)。

第二步：接着需要对xaid对应的dt进行过滤，获取xaid中最大的dt，判断这个dt是否等于（当天日期-7），如果满足条件，则说明这个用户最近7日内没有使用app，就认为他属于7日流失用户。

> 举个例子：dws_user_active_history表中有以下几条数据
>
> xaid					dt
>
> a1						2026-02-01
>
> a1						2026-02-05
>
> b1						2026-02-01
>
> b1						2026-02-02
>
> c1						2026-02-03
>
>  
>
> 针对这份数据，我们想要在02-09号统计用户7日流失量
>
> 那也就意味着要统计里面在02-02号使用过APP，但是在之后的7天内，一直到02-09号都没有再使用过app的用户。
>
>  
>
> 根据xaid进行分组，获取里面最大的日期（最近一次使用app的时间）
>
> a1					2026-02-01,2026-02-05
>
> b1					2026-02-01,2026-02-02
>
> c1					2026-02-03
>
> 
>
> 判断这个时间是否等于02-02，如果满足这个条件，就说在02-09号之前的7天内没有使用过app。
>
> 这里的b1满足条件，所以它就是7日流失用户了。

第三步：将满足条件的xaid数据保存到dws层的 *dws_user_lost_item* 表中。

第四步：对 *dws_user_lost_item* 表中的数据进行聚合统计，统计用户7日流失数据量，保存到APP层的 *app_user_lost_count* 表中。

### 1.7.2、脚本执行之DWS层

1：表初始化脚本（初始化执行一次）

```bash
# 初始化ods库与表
[emon@emon ~]$ sh /home/emon/bigdata/warehouse/shell/sqoop/userAction/dws_init_table_3.sh
```

2：添加分区数据脚本（每天执行一次）

```bash
# 添加分区：20260201-20260209
[emon@emon ~]$ sh /home/emon/bigdata/warehouse/shell/sqoop/userAction/tmp_load_dws_data_3.sh
```

### 1.7.3、脚本执行之APP层

1：表初始化脚本（初始化执行一次）

```bash
# 初始化ods库与表
[emon@emon ~]$ sh /home/emon/bigdata/warehouse/shell/sqoop/userAction/app_init_table_3.sh
```

2：添加分区数据脚本（每天执行一次）

```bash
# 添加分区：20260201-20260209
[emon@emon ~]$ sh /home/emon/bigdata/warehouse/shell/sqoop/userAction/tmp_load_app_data_3.sh
```

## 1.8、需求4：每日启动App次数相关指标

### 1.8.1、指标1：每日人均启动APP次数

每日人均启动APP次数=当日所有用户启动APP总次数/当日所有人数

实现思路：

第一步：基于 *dws_user_active_history* 表，统计当日的数据，根据times字段的值求pv和uv即可。

第二步：将计算的结果到APP层的 *app_user_open_app_count* 表。

### 1.8.2、指标2：每日APP启动次数分布（1次2次3次及以上）

实现思路：

对 *dws_user_active_history* 里面的times字段进行统计，计算times=1的数据条数、times=2的数据条数以及times>=3的数据条数即可，将最终的结果保存到APP层的 *app_user_open_app_distrib*。

### 1.8.3、脚本执行之APP层

1：表初始化脚本（初始化执行一次）

```bash
# 初始化ods库与表
[emon@emon ~]$ sh /home/emon/bigdata/warehouse/shell/sqoop/userAction/app_init_table_4.sh
```

2：添加分区数据脚本（每天执行一次）

```bash
# 添加分区：20260201-20260209
[emon@emon ~]$ sh /home/emon/bigdata/warehouse/shell/sqoop/userAction/tmp_load_app_data_4.sh
```

## 1.9、需求5：操作系统活跃用户相关指标

### 1.9.1、指标1：操作系统活跃用户分布（安卓、IOS）

### 1.9.2、指标2：安卓系统版本活跃用户分布

### 1.9.3、指标3：IOS系统版本活跃用户分布

### 1.9.4、指标4：设备品牌活跃用户分布

### 1.9.5、指标5：设备型号活跃用户分布

### 1.9.6、指标6：网络类型活跃用户分布

针对以上6大指标，其实主要就是针对 *dwd_user_active* 表中的这些相关维度字段进行分组聚合统计。

实现思路：

第一步：利用咱们前面讲的维度建模的思想，使用星型模型，基于 *dwd_user_active* 表，在外层构建对应的维度表。

第二步：在DWS层基于以上6种维度创建对应的维度聚合表，按天创建分区。

对应的表名为：

> *dws_user_platform_distrib*
>
> *dws_user_android_osver_distrib*
>
> *dws_user_ios_osver_distrib*
>
> *dws_user_brand_distrib*
>
> *dws_user_model_distrib*
>
> *dws_user_net_distrib*

第三步：基于DWS层的轻度聚合数据进行全局聚合，因为这些指标统计的时候需要统计所有数据，只统计某一天的没有多大意义，将最终聚合的结果保存到APP层，这里面的表就是普通的外部表了，里面也不需要日期字段，每天重新生成表里面的数据即可。

注意了：咱们前面保存的有每天聚合的数据，如果后期有需求要统计一段时间内的这些维度的指标，那也很简单，直接基于DWS层的表进行统计即可，从这也提现出来了数据分层的好处。

在APP层对应的表名为：

> *app_user_platform_distrib*
>
> *app_user_android_osver_distrib*
>
> *app_user_ios_osver_distrib*
>
> *app_user_brand_distrib*
>
> *app_user_model_distrib*
>
> *app_user_net_distrib*

### 1.9.7、脚本执行之DWS层

1：表初始化脚本（初始化执行一次）

```bash
# 初始化ods库与表
[emon@emon ~]$ sh /home/emon/bigdata/warehouse/shell/sqoop/userAction/dws_init_table_5.sh
```

2：添加分区数据脚本（每天执行一次）

```bash
# 添加分区：20260201-20260209
[emon@emon ~]$ sh /home/emon/bigdata/warehouse/shell/sqoop/userAction/tmp_load_dws_data_5.sh
```

### 1.9.7、脚本执行之APP层

1：表初始化脚本（初始化执行一次）

```bash
# 初始化ods库与表
[emon@emon ~]$ sh /home/emon/bigdata/warehouse/shell/sqoop/userAction/app_init_table_5.sh
```

2：添加分区数据脚本（每天执行一次）

```bash
# 重新统计全量数据
[emon@emon ~]$ sh /home/emon/bigdata/warehouse/shell/sqoop/userAction/app_add_partition_5.sh
```

# 三、电商数据仓库之商品订单数仓

