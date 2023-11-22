---
toc: content
order: 30317
---

# JOIN

## Join

Join用来将两张或多张表通过某些字段合并成一张大表。目前支持了InnerJoin, LeftJoin，SemiJoin。实现上JoinOp在Searcher上运行，需要业务方保证不同表的数据可以根据join key分布在相同索引分片。

### 语法格式

```
select:
  SELECT [ ALL | DISTINCT ]
  { * | projectItem [, projectItem ]* }
  FROM tableExpression
  
tableExpression:
  tableReference [, tableReference ]*
  | tableExpression [ INNER | LEFT ] JOIN tableExpression [ joinCondition ]
  
joinCondition:
  ON booleanExpression
```

### 示例
1. 两表join

```
SELECT
	t1.id, t2.id
FROM
	tj_shop AS t1
JOIN
	tj_item AS t2
ON
	t1.id = t2.id
```

2. 多表join

```
SELECT
    ti.id, ts.id, tr.price
FROM
    tj_item AS ti
JOIN
    tj_relation AS tr
ON
    ti.id = tr.item_id
JOIN
	tj_shop AS ts
ON
	tr.shop_id = ts.id
```

### 注意
1. 两张分区表join， 要求join字段必须包含Hash字段，用来保证可以在Searcher上执行join。用户可以关闭此检查，参考[kvpair子句](../kvpair-clause.md)。
2. 分区表和广播表join， 或者两张广播表join，没有join字段的限制。
3. 在用户保证数据分布的情况下，支持单值字段和多值字段join。
* 使用 MULTICAST ，将多值字段转换为单值字段，再join

```
SELECT t2.id
FROM
(
  SELECT multi_id
  FROM tj_shop
) AS t1
JOIN
	tj_item AS t2
ON
	MULTICAST(t1.multi_id) = t2.id
```

## JOIN操作类型

### Left Outer Join
* 左连接。返回左表中的所有记录，即使右表中没有与之匹配的记录。
```
SELECT
  t1.id, t2.id
FROM
  tj_shop AS t1
LEFT JOIN
  tj_item AS t2
ON
  t1.id = t2.id
```

### Inner Join
* 内连接。关键字inner可以省略。返回满足ON条件的行；
```
SELECT
  t1.id, t2.id
FROM
  tj_shop AS t1
JOIN
  tj_item AS t2
ON
  t1.id = t2.id
```

* 支持ON条件恒为TRUE，此时返回两表的笛卡尔积；下述两条SQL等价；
```
SELECT
  t1.id, t2.id
FROM
  tj_shop AS t1
JOIN
  tj_item AS t2
ON
  TRUE

SELECT
  t1.id, t2.id
FROM
  tj_shop, tj_item;
```

### Semi Join
* 半连接，SEMI JOIN中，右表只用于过滤左表的数据而不出现在结果集中。
* 当join条件成立时，返回左表中的数据。如果tj_shop中某行的id在tj_item的所有id中出现过，则此行保留在结果集中。

```
SELECT
  id
FROM
  tj_shop
WHERE id IN (
  SELECT 
    id 
  FROM 
    tj_item
)
```

```
SELECT
  id
FROM
  tj_shop
WHERE EXISTS (
  SELECT 
    id 
  FROM 
    tj_item
  WHERE
    tj_shop.id = id
)
```

### Anti Join
* 反连接，Anti JOIN中，右表只用于过滤左表的数据而不出现在结果集中。
* 当join条件不成立时，返回左表中的数据。如果tj_shop中某行的id在tj_item的所有id中不存在，则此行保留在结果集中。

```
SELECT
  id
FROM
  tj_shop
WHERE id NOT IN (
  SELECT 
    id 
  FROM 
    tj_item
)
```

```
SELECT
  id
FROM
  tj_shop
WHERE NOT EXISTS (
  SELECT 
    id 
  FROM 
    tj_item
  WHERE
    tj_shop.id = id
)
```

## JOIN实现类型

### Nested Loop Join
* 嵌套循环join，对两表进行笛卡尔积展开再过滤，主要用于非等值join。
```
SELECT
  t1.id, t2.id
FROM
  tj_shop AS t1
JOIN
  tj_item AS t2
ON
  t1.id > t2.id
```

* <strong>说明</strong> 数据会膨胀，性能比较差
* <strong>说明</strong> 链接的表类型为KV/KKV/Summary时，该表的查询条件中必须包含PK等值条件，参考“[使用限制](./from/kv-kkv-query.md#3-使用限制)。

### Hash Join
* 用于包含等值条件的join
* 将小表按连接键计算出一个hash表，然后从大表一条条抽取记录，计算hash值，根据hash到A表的hash来匹配符合条件的记录。

```
SELECT
  t1.id, t2.id
FROM
  tj_shop AS t1
JOIN
  tj_item AS t2
ON
  t1.id = t2.id
```

* <strong>说明</strong> 默认生成的优化计划不一定是hash join，可通过[hint](./Hint.md#内置hint)指定hash join
* <strong>说明</strong> 链接的表类型为KV/KKV/Summary时，该表的查询条件中必须包含PK等值条件，参考“[使用限制](./from/kv-kkv-query.md#3-使用限制)”。


### Lookup Join
* 用于包含等值条件的join
* 将小表按连接键作为大表的查询条件，去召回大表纪录。

```
SELECT
  t1.id, t2.id
FROM
  tj_shop AS t1
JOIN
  tj_item AS t2
ON
  t1.id = t2.id
```


* <strong>说明</strong> 等值条件的一边是建索引的字段才可以开启lookup join优化
* <strong>说明</strong> 支持等值条件部分字段建索引
* <strong>说明</strong> iquan默认生成的优化计划不一定是lookup join，可通过[hint](./Hint#内置hint)指定lookup join
* <strong>说明</strong> 链接的表类型为KV/KKV/Summary时
  * 该表的查询条件中必须包含PK等值条件，参考“[使用限制](./from/kv-kkv-query#3-使用限制)”。
  * 或者，join的等值条件部分包含PK字段