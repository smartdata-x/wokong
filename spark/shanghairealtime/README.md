# 上海电信实时数据接收
该程序是运行在上海电信平台，负责将解析的原始、search和visit数据发送到kafka。电信方会根据从kafka中接到的消息的内容将数据分别写入到两张kv表

**原始**:  kunyan_to_upload_inter_tab_up

**search和visit**: kunyan_to_upload_inter_tab_sk

## 注意事项

* 当原始数据接收规则发生更改，比如：有新的url内容需要解析，需要将最新的url列表发送给电信方负责人，及时更新hdfs上的url匹配列表

* 当search和visit数据的匹配规则需要修改，需要将程序中对应的匹配规则数组进行更新，重新构建最新的jar发送到电信方，更新最新jar包

以上两个变动都需要将实时spark程序重新启动

