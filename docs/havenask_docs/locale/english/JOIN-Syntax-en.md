## Description

You can use the JOIN operation to merge two or more tables into a large table based on common fields. Supported JOIN operations include INNER JOIN, LEFT JOIN, and SEMI JOIN. For the JOIN operator to be executed on Searcher, you must make sure that the data of different tables can be aggregated in the same index shard based on the join key.



## Syntax



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



## Examples

1. The following example shows how to join two tables:



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



2. The following example shows how to join more than two tables:



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



## Additional considerations:

1. When you join two partition tables, you must ensure that the fields you want to join contain hash fields. This way, the JOIN operation can be executed on Searcher. You can disable this check. For more information, see [kvpair clause](https://github.com/alibaba/havenask/wiki/kvpair-clauses-en).

2. When you join a partition table and a broadcast table or two broadcast tables, you can specify the join field of any data type.

3. When the data that you want to join is stored in the locations that Searcher workers can access, you can join single-value fields and multi-value fields.

* You can use multicast to convert a multi-value field to a single-value field before you perform the JOIN operation.



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
