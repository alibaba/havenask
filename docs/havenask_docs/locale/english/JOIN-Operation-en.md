### Left Outer Join

* LEFT JOIN is the operator for left outer join operations. This type of join operation returns all data entries in the left table, including the rows that do not match those in the right table.

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

* INNER JOIN is the operator for inner join operations. When you specify the operator, specify only JOIN and omit the INNER keyword. An inner join operation returns the rows of data that matches the ON condition.

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

* A LEFT SEMI JOIN operation returns rows from the left table that has matching rows in the right table. Example: If the value of the id field of a row in the tj_shop table is the same as a value of the id field in the tj_item table, the row of data is included in the result set.



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

* A LEFT ANTI JOIN operation returns rows from the left table that does not have matching rows in the right table. Example: If the value of the id field of a row in the tj_shop table is not a value of the id field in the tj_item table, the row of data is included in the result set.



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
