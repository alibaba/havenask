---
toc: content
order: 30319
---

# 动态参数
## 描述
动态参数类似的数据库中的[PrepareStatement](https://en.wikipedia.org/wiki/Prepared_statement)。用户可以在SQL中设置placeholder（?表示），同时传递placeholder对应的值。Iquan内部会自动替换。

动态参数主要用于提高Plan Cache命中率，对于查询模式固定的场景性能提升明显。

注意动态参数只能替换值，不支持关键字或字段替换。

## 参数说明

|字段 | 含义 | 示例 | 
|-- | -- | -- | 
iquan.plan.prepare.level | 对指定阶段的结果使用具体的参数替换"?"号。 默认为jni.post.optimize。 |kvpair=...;iquan.plan.prepare.level:jni.post.optimize;... | 
dynamic_params | 填入要替换的具体值。 注意: 类型一致。注意: 动态参数是一个二维数组；其中每一个一维数组对应一条SQL。 | kvpair=...;dynamic_params=[[1, 1.23, "str"]] |
iquan.plan.cache.enable | 根据iquan.plan.prepare.level启用不同的cache。在动态参数下，如果启动了cache功能，iquan会将带"?"的SQL对应的执行计划放入cache, 下次再访问同样的SQL时候，可以直接从cache中取出plan，极大的节省了时间。 | kvpair=...;iquan.plan.cache.enable:true;... | 

## 示例
1. 只启用动态参数：
```
SELECT i1, cast(? as bigint) FROM t1 WHERE (i2 > 5 AND d3 < 10.1) OR s5 = ?
```
为了替换掉SQL中的动态参数（也就是"?"）, 用户需要通过kvpair传入具体的参数，具体如下：

```
kvpair=...;
			 iquan.plan.prepare.level:jni.post.optimize;
       dynamic_params:[[10, "str5"]];
       ...;
```
2. 同时启用了cache和动态参数：

```
SELECT
    price,
    title,
    compute(
        longitude,
        latitude,
        city_id,
        CAST(? AS double),
        CAST(? AS double),
        CAST(1 AS bigint)
    ) AS distance
FROM
    store,
    unnest(store.sub_table)
WHERE
    MATCHINDEX('shop', ?)
    AND QUERY(name, ?) 
```

kvpair参数：
```
kvpair= ...;
			 iquan.plan.cache.enable:true;
			 iquan.plan.prepare.level:jni.post.optimize;
       dynamic_params:[[119.98844256998, 
                        36.776817017143, 
                        "excellect", 
                        "水果 OR 西瓜"]]
       ...
```
