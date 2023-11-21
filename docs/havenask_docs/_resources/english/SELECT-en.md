### Description

SELECT statements are used to select data from tables.



SELECT statements are the core of the query syntax. All advanced query syntax is built by using SELECT statements. This section describes the simple syntax of SELECT statements supported by Havenask. For information about how to use advanced SELECT syntax, see other sections.



### Syntax

```

SELECT:

  SELECT [ DISTINCT ]

   { * | projectItem [, projectItem ]* }

  FROM tableExpression

   [ WHERE booleanExpression ]

   [ GROUP BY { groupItem [, groupItem ]* } ]

    [ ORDER BY { orderByItem [, OrderByItem ]* }]

   [ HAVING booleanExpression ]

    [ LIMIT number]

    [ OFFSET number]



projectItem:

  expression [ [ AS ] columnAlias ]

  | tableAlias . *

```



### Examples

```

SELECT * FROM table;



SELECT f1, f2 AS ff FROM table;



SELECT DISTINCT f FROM table;



SELECT * FROM (

      SELECT f1, count(*) AS num

      FROM t1

      GROUP BY f1

  ) tt

  WHERE tt.num > 100;

```
