## 描述
Join用来将两张或多张表通过某些字段合并成一张大表。目前支持了InnerJoin, LeftJoin，SemiJoin。实现上JoinOp在Searcher上运行，需要业务方保证不同表的数据可以根据join key分布在相同索引分片。

## 语法格式

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

## 示例
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

## 注意
1. 两张分区表join， 要求join字段必须包含Hash字段，用来保证可以在Searcher上执行join。用户可以关闭此检查，参考[kvpair子句](./kvpair子句.md)。
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