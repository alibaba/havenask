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



| Item | Expression | Example |

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

SQL allows you to use inverted indexes of Havenask to accelerate queries in WHERE clauses. For example, MATCHINDEX and QUERY are compatible with the syntax and equivalence conditions of Havenask queries, and equivalent conditions. Example: `SELECT * FROM table WHERE f1 = 10`. If f1 is used as an equivalent condition to create inverted indexes, the system can automatically optimize the query as an inverted query. scan op can automatically extract the query conditions that can be extracted.




