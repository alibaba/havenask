---
toc: content
order: 0
---

# 使用限制
* 目前不支持的DDL及DML语法
* ORDER BY子句必须和LIMIT配对使用
* 使用UNNEST子句查询子表时，不支持SELECT *
* SQL目前不支持无符号类型，建议配置HA3 Schema时不要使用无符号类型，可能会存在类型转换的问题
* JOIN目前只支持在Searcher上执行，需要业务方保证数据分布
* UNION需要保证两边的字段名称、字段类型、字段数量均要一致
* DATE和RANGE索引不支持使用"="查询，可以使用`WHERE QUERY('consign_time', '[1, 10]')`这样的方式来查询。
