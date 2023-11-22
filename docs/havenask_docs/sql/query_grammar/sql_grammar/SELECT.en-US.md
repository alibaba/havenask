---
toc: content
order: 10
---

# SELECT
## Description
SELECT statements select data from tables.

SELECT statements are the core of the query syntax. All advanced query syntax is used based on SELECT statements. This topic describes the SELECT statement syntax supported by HAVENASK. For more information about how to use HAVENASK, see the other sections.

## Syntax
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

## Examples
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
