---
toc: content
order: 30317
---

# JOIN

## Join

Join is used to merge two or more tables into one large table by using certain fields. Valid values: InnerJoin, LeftJoin, and SemiJoin. To implement JoinOp on Searcher, the business side needs to ensure that the data of different tables can be distributed in the same index shard according to the join key.

### Syntax

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

### Examples
1. This example shows how to join the table films with the table distributors:

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

2. Multi-table join

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

### Note
1. The two partition table are joined. The join field must contain a hash field to ensure that the join can be executed on Searcher. You can disable this check. For more information, see [kvpair clause](./kvpair clause. md).
2. A partition table and broadcast table join, or two broadcast tables join, with no restrictions on the join field.
3. Single-value field and multi-value field join are supported when users guarantee data distribution.
* Use MULTICAST to convert a multi-value field to a single-value field, and then join

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

## Join operation types

### Left Outer Join
* LEFT JOIN is the operator for left outer join operations. This type of join operation returns all data entries in the left table, including the rows that do not match the rows in the right table.
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
* INNER JOIN is the operator for inner join operations. The keyword inner can be omitted. An inner join operation returns the rows of data that match the ON condition.
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

* You can set the ON condition to TRUE. If you set the condition to TRUE, the system returns the Cartesian products that the system calculates based on the number of values in the specified fields in the tables. The SQL queries that are defined in the following sample code return the same result.
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
* SEMI JOIN is the operator for semi-join operations. When the system performs a semi-join operation, the system filters data in the left table based on the data in the right table. Data in the right table is not included in the dataset that the system returns after the system performs the semi-join operation.
* If the join condition is true, the data in the left table is returned. If the id of a row in tj_shop appears in all the ids of tj_item, the row remains in the result set.

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
* ANTI JOIN is the operator for anti-join operations. When the system performs an anti-join operation, the system filters data in the left table based on the data in the right table. Data in the right table is not included in the dataset that the system returns after the system performs the anti-join operation.
* If the join condition does not hold, the data in the left table is returned. If the id of a row in tj_shop does not exist in all the ids of tj_item, the row remains in the result set.

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

## Join methods

### Nested Loop Join
* When the system performs a nested-loop join on two tables, the system first finds the Cartesian product of the tables, expands and filters the results, and then performs a non-equi join.
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

* <strong>This indicates that the </strong>data will expand and the performance is poor.
* <strong>Note: If the type of the table to which the </strong>link is linked is KV, KKV, or Summary, the query condition of the table must contain the PK equivalent condition. For more information, see [Limits](./KV, KVV query. md#3-Limits).

### Hash Join
* You can use this join method to join values based on equivalent conditions.
* When the system performs a hash join operation, the system calculates hash values of data rows in the small table based on the values in the primary key field of the small table and calculates hash values of data rows in the big table based on the values in the primary key field of the big table. The system compares the hash values of the small table and the hash values of the big table, and then joins the tables based on the specified condition.

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

* <strong>Note: The default optimization plan generated by </strong>is not necessarily a hash join. You can specify a hash join by [hint](./Hint.md# Built-in hints)
* <strong>Note: If the type of the table to which the </strong>link is linked is KV, KKV, or Summary, the query condition of the table must contain the PK equivalent condition. For more information, see [Limits](./KV, KVV query. md#3-Limits).


### Lookup Join
* You can use this join method to join values based on equivalent conditions.
* You can use the primary key values of a small table as conditions to query and retrieve data entries from a big table.

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


* <strong>indicates that lookup join optimization can only be enabled </strong>one side of the equivalence condition is an indexed field.
* <strong>Note: </strong>supports indexing of some fields with equivalent conditions.
* <strong>Note: The default optimization plan generated by </strong>iquan is not necessarily lookup join. You can specify lookup join by [hint](./Hint# Built-in hints)
* <strong>Description </strong>When the type of the linked table is KV, KKV, or Summary
   * The query condition of the table must contain the PK equivalent condition. For more information, see [Limits](./kv-kkv-query#3-Restrictions).
   * Or, the equivalent condition part of the join contains the PK field