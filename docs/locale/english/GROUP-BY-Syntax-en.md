## Overview

GROUP BY is used to aggregate one or more fields to calculate the metrics for the required data such as the maximum value, minimum value, and average value.



## Syntax

```

select:

    SELECT [ ALL | DISTINCT ]

    { * | projectItem [, projectItem ]* }

    FROM tableExpression

    [ WHERE booleanExpression ]

    [ GROUP BY { groupItem [, groupItem ]* } ]

    [ HAVING booleanExpression ]

```



## Built-in UDAF functions

The following table describes built-in user-defined aggregate functions (UDAF).



| No. | Function name | Feature |

-- | -- | --|

| 1 | SUM | Obtains the sum after aggregation. |

| 2 | MIN | Obtains the minimum value after aggregation. |

| 3 | MIN | Obtains the maximum value after aggregation. |

| 4 | COUNT | Obtains the number of data records. |

| 5 | AVG | Obtains the average value after aggregation. |

| 6 | ARBITRARY | Selects any input value as the output result. |



## Examples

1. sum



```

SELECT brand, SUM(price) FROM phone WHERE nid < 8 GROUP BY brand

```



2. min



```

SELECT brand, MIN(price) FROM phone WHERE nid < 8 GROUP BY brand

```



3. max



```

SELECT brand, MAX(price) FROM phone WHERE nid < 8 GROUP BY brand

```



4. count



```

SELECT brand, COUNT(*) FROM phone WHERE nid < 8 GROUP BY brand

```



5. avg



```

SELECT brand, AVG(price) FROM phone WHERE nid < 8 GROUP BY brand

```



6. HAVING



```

SELECT brand FROM phone GROUP BY brand HAVING COUNT(brand) > 10

```
