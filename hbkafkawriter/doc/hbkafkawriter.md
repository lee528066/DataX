# DataX KafkaWriter


---

## 1 快速介绍

把postgresql的数据按照指定的json格式导入Kafka的插件

## 2 实现原理

使用kafka-client的Producer，将postgresql的数据转化成指定的json格式，将其发送到指定的topic中。

kafka消息的key：
```
    table+":"+{$主键的值}
```

kafka消息的value(json格式）：
```$json
{
  "data": {
    "name": "liwei",
    "age": 18,
    "id": 1
  },
  "primaryKey": "id",
  "table": "student"
}
```

## 3 功能说明

### 3.1 配置样例

#### job.json

```
{
  "name": "hbkafkawriter",
  "parameter": {
    "topic": "",
    "bootstrapServers": "localhost:9092",
    "username": "xx",
    "password": "xx",
    "jdbcUrl": "jdbc:postgresql://127.0.0.1:3002/datax",
    "table": "test"
  }
}
```

#### 3.2 参数说明

* topic
 * 描述：kafka topic名称
 * 必选：是
 * 默认值：无

* bootstrapServers
 * 描述：kafka broker集群地址
 * 必选：是
 * 默认值：无

* jdbcUrl
 * 描述：postgresql jdbcUrl（和postgresqlReader保持一致）
 * 必选：是
 * 默认值：空

* username
 * 描述：postgresql的登录用户名
 * 必选：是
 * 默认值：无

* password
 * 描述：postgresql的登录用户名
 * 必选：是
 * 默认值：无

* table
 * 描述：当前传输的表
 * 必选：是
 * 默认值：无