# 电信 spark 程序集合
## 结构说明
---
## shanghai
- shanghairealtime (跑在上海电信服务器上的过滤实时原始数据和热度数据的程序)

### raw
- shanghaitelecom (跑在公司服务器上的从公司的 kafka 中获取 kv 表里的实时原始数据)

### stock
- sh_spark (旧的上海电信离线热度数据匹配规则)
- SHOffLineStock.java (跑在上海电信服务器上的过滤离线热度数据的程序)
- shanghaisearchspark (测试中跑在上海电信服务器上的过滤离线热度数据的程序)
- shdx_kv_down (跑在公司服务器上的接离线热度数据的程序)
- stockheat （新版悟空离线热度数据分析程序）
- telecomDataAnalysis （旧版悟空离线热度数据分析程序，杨阁在用，用途不明）

---
## jiangsu
- jiangsusearchandvisit （跑在江苏电信服务器上的过滤实时热度数据的程序）
- JSAndZJOfflineStock.java （跑在江苏和浙江电信服务器上的过滤离线热度数据的程序）

---
## guangzhou
- gztelecomtest （跑在广州电信服务器上的测试程序）

---
## deprecated
- alertStockcode （关注热度大涨提醒程序）
- followDataAnalysis （旧版悟空的股票关注数据统计程序）
- hotwords （热词程序）
