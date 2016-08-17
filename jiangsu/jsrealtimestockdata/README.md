# 江苏电信搜索数据接收
该程序是运行在江苏电信平台，负责将search和visit数据发送到kafka，从电信kafka取出的数据写入kv中
写回的kafka topic：kafka2kv

**search和visit**: 数据保存到kv的表名：kunyan_spark_tab

## 注意事项

* 当search和visit数据的匹配规则需要修改，需要将程序中对应的匹配规则数组进行更新，重新构建最新的jar发送到电信方，更新最新jar包

如有变动需要将spark程序重新启动

jar版本控制：
1.5 ： 正式版本
1.6： 写入kafka数据，同时写到hdfs，方便对比数据量
1.7： 注释程序中的配置参数