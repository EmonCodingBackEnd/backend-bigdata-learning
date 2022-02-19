# 示例与表

## offsets相关表
- 创建表
```bash
create 'offsets_storage','o'
```

## 示例1

- 创建表： HBaseClient

```
create 'user','o'
```

- 创建数据：

```
put 'user','1','o:name','lm'
put 'user','1','o:age',20
put 'user','1','o:sex',0

put 'user','2','o:name','lm2'
put 'user','2','o:age',30
put 'user','2','o:sex',0
```

## 示例2

- 创建表：StreamingApp

```
create 'access_user_hour','o'
```

## 示例3

- 创建表：StreamingApp

```
create 'access_user_day','o'
```
