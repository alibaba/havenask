## 描述
Summary查询用于兼容havenask两阶段查询。havenask表注册到Iquan时会额外注册一张Summary表，Summary表表名为Schema中配置的表名加上后缀（默认为_summary_，可配置），Summary查询均需要使用此表。

Summary查询时必须包含PK条件。


## 示例
```
SELECT brand, price FROM phone_summary_ WHERE nid = 8 or nid = 7

SELECT brand, price FROM phone_summary_ WHERE nid IN (7, 8, 9)
```

## 查询条件限制
Summary查询中， `where` 子句后的条件必须含有pk字段。如上述示例中， `phone` 表的pk字段即为 `nid` ，用于召回对应的数据条目。

在确保查询条件中含有pk字段后，还可以通过 `AND` 语句叠加其他的条件，对召回的宝贝进行筛选

`SELECT brand, price FROM phone_summary_ WHERE nid IN (7, 8, 9) AND price < 2000`