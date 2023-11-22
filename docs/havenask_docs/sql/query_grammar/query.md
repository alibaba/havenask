---

toc:  content
order: -1
---

# 查询方式
sql目前支持query子句和kvpair子句，query子句用于拼写sql query，kvpair子句用于拼写一些相关参数。
与ha3不同，sql的查询的path为"/sql"，而ha3是"/"

## 执行查询
在知道HA3 Qrs的服务IP和端口的前提下，用户可以使用curl 指令手动调用查询接口。

```
curl "ip:port/sql?query=SELECT brand, COUNT(*) FROM phone GROUP BY (brand)&&kvpair=trace:INFO;formatType:json"
```

## 注意
kvpair和&&之间不允许留空格，以及kvpair多项之间不允许空格，否则将解析无效。

## 查看集群注册信息
用户可以curl集群qrs的\<host\>:\<ip\>/sqlClientInfo查看集群中注册的表和函数