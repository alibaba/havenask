---
toc: content
order: 30319
---

# Dynamic parameters
## Description
Dynamic parameters are similar to [PrepareStatement](https://en.wikipedia.org/wiki/Prepared_statement) in the database. You can use question marks (?) as placeholders in an SQL statement to specify dynamic parameters. The Iquan component of OpenSearch Vector Search Edition automatically converts the question marks (?) to the corresponding dynamic parameters.

Dynamic parameters can help improve the hit ratio of execution plans that the system caches. If you write SQL statements based on the same template and use dynamic parameters in the SQL statements, query performance is increased.

Take note that you can use dynamic parameters to replace only values. You cannot use dynamic parameters to replace keywords or fields.

## Parameters

| Field | Meaning | Example |
|-- | -- | -- |
iquan.plan.prepare.level | Replace the "?" sign with a specific argument for the outcome of the specified stage.  Default value: jni.post.optimize  |kvpair=...;iquan.plan.prepare.level:jni.post.optimize;... |
dynamic_params | Enter the specific value to be replaced.  Note: The types are consistent. Note: Dynamic parameters are a two-dimensional array. Each one-dimensional array corresponds to an SQL statement.  | kvpair=...;dynamic_params=[[1, 1.23, "str"]] |
iquan.plan.cache.enable | Enable different caches depending on the iquan.plan.prepare.level. If you use a dynamic parameter in an SQL statement and enable the execution plan cache feature, OpenSearch Vector Search Edition caches the execution plan of the SQL statement. When you use the same SQL statement again, OpenSearch Vector Search Edition obtains the execution plan from the cache. In this case, the query execution time is reduced.  | kvpair=...;iquan.plan.cache.enable:true;... |

## Examples
1. In the following sample statement, only dynamic parameters are used.
```
SELECT i1, cast(? as bigint) FROM t1 WHERE (i2 > 5 AND d3 < 10.1) OR s5 = ?
```
Before you use question marks (?) to specify dynamic parameters in an SQL statement, you must create dynamic parameters by using a kvpair clause. The following sample code shows how to specify dynamic parameters in a kvpair clause:

```
kvpair=...;
			 iquan.plan.prepare.level:jni.post.optimize;
       dynamic_params:[[10, "str5"]];
       ...;
```
2. In the following sample statement, dynamic parameters are used and the execution plan cache feature is enabled.

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

The following sample code shows how to specify dynamic parameters in a kvpair clause:
```
kvpair= ...;
			 iquan.plan.cache.enable:true;
			 iquan.plan.prepare.level:jni.post.optimize;
       dynamic_params:[[119.98844256998,
                        36.776817017143,
                        "excellect",
                        "Fruit OR Watermelon"]]
       ...
```
