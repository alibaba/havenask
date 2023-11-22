#### Nested Loop Join

* When the system performs a nested-loop join operation, the system joins values from the left table and the right table, and then filters the results. The number of paths that the system accesses to obtain values from the tables is a Cartesian product that is calculated based on the number of values in the specified field in the left table and the number of values in the specified field in the right table. You can use the nested-loop join method to join values based on inequivalent conditions.

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



* <strong></strong>The data will expand, resulting in poor performance.

* <strong></strong> If a table that you specify for a join operation is a key-value table, Pkey-Skey-value table, or summary table, specify an equivalent condition based on the primary key field of the table as a query condition to query data from the table. For more information, see [Usage restrictions](https://github.com/alibaba/havenask/wiki/KV-Query-en#3-limits).



#### Hash Join

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



* <strong></strong>The default join method that the system uses in optimization plans may not be a hash join method. You can use a hint to specify hash join as the default join method. For more information, see [Hints](https://github.com/alibaba/havenask/wiki/Hint-en).

* <strong></strong>If a table that you specify for a join operation is a key-value table, Pkey-Skey-value table, or summary table, specify an equivalent condition based on the primary key field of the table as a query condition to query data from the table. For more information, see [Query data from a key-value table or Pkey-Skey-value table](https://github.com/alibaba/havenask/wiki/KV-Query-en)。





#### Lookup Join

* You can use this join method to join values based on equivalent conditions.

* You can use the primary key values of a small table as conditions to query data entries from a big table.



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





* <strong></strong>When you perform a lookup join operation to optimize a query, make sure that one of the operands in the equivalent condition that you specify is an indexed field.

* <strong></strong>You can create indexes based on specific fields that you specify in the equivalent condition.

* <strong></strong>The default join method that the system uses in optimization plans may not be a lookup join method. You can use a hint to specify lookup join as the default join method. For more information, see [Hints](https://github.com/alibaba/havenask/wiki/Hint-en).

* <strong></strong>If a table that you specify for a join operation is a key-value table, Pkey-Skey-value table, or summary table,

   * you must specify an equivalent condition based on the primary key field of the table as a query condition to query data from the table. For more information, see [Usage restrictions](https://github.com/alibaba/havenask/wiki/KV-Query-en#3-limits).

   * You can also include the primary key field of the table in the equivalent condition based on which you want the system to perform the join operation.