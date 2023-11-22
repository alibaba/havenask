---
toc: content
order: 30313
---

# WHERE
## Description
You can use WHERE clauses to select data from a table.

## Syntax

```
select:
  SELECT [ DISTINCT ]
    { * | projectItem [, projectItem ]* }
  FROM tableExpression
    [ WHERE booleanExpression ]
```

The following table describes the types of booleanExpression.

| No. | Operator | instance |
|---|---|---|
| 0 | AND, OR | WHERE a > 1 AND a < 100, WHERE a > 5 OR c > 100 |
| 1 | >, >=, <, <=, <> |
| 2 | IN | WHERE id IN (1, 2, 3, 4, 5) |

## Examples

```
SELECT * FROM table WHERE f1 > 10 AND f2 < 5

SELECT * FROM table WHERE id IN (5, 6, 7, 8, 9)
```
## Hint
SQL supports WHERE to use the inverted optimization of havenask to accelerate searches, such as MATCHINDEX,QUERY is compatible with the syntax implementation of havenask queries, and equivalent conditions, such as `SELECT * FROM table WHERE f1 = 10`. When f1 is the equivalent condition for creating inverted indexes, it can be automatically optimized to inverted searches. scan op can automatically extract the query conditions that can be extracted.


