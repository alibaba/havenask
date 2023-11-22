---
toc: content
order: 30314
---

# GROUP BY
## Group-BY
GROUP BY is used to aggregate one or more fields to calculate the metrics for the required data, such as the maximum value, minimum value, and average value.

### Syntax
```
select:
    SELECT [ ALL | DISTINCT ]
    { * | projectItem [, projectItem ]* }
    FROM tableExpression
    [ WHERE booleanExpression ]
    [ GROUP BY { groupItem [, groupItem ]* } ]
    [ HAVING booleanExpression ]
```

### Built-in UDAFs
The following table describes built-in user-defined aggregate functions (UDAFs).

| Sequence number | Function name | Function |
-- | -- | --|
| 1 | SUM | Sum after aggregation |
| 2 | MIN | Find the minimum value after aggregation |
| 3 | MAX | Find the maximum value after aggregation |
| 4 | COUNT | Number of statistics |
| 5 | AVG | Calculate the average after aggregation |
| 6 | ARBITRARY | Select any input value as the result output |

### Examples
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

6. having clause

```
SELECT brand FROM phone GROUP BY brand HAVING COUNT(brand) > 10
```

## GROUPING-SET
GROUPING SETS is an extension of GROUP BY clauses. You can use GROUPING SETS to group query results by using multiple methods. This way, you do not need to execute multiple SELECT statements to group query results. High-performance Search Edition can provide a simpler execution plan when you use GROUPING SETS clauses. This helps improve the performance of query execution.

### Examples
```
SELECT brand,size,SUM(price) AS sp
FROM phone
GROUP BY GROUPING SETS ((brand),(size),())
```

The preceding scenario in which the GROUPING SETS clause is used is a common usage scenario. If you want to calculate the prices of mobile phones by brand and size, and calculate the total price of all mobile phones in the mobile phone table, you can use the GROUP BY clause that contains the brand and size subsets, empty subset, and space. brand specifies to calculate the prices of mobile phones by brand. size specifies to calculate the prices of mobile phones by size. The empty subset specifies to calculate the total price of all mobile phones. The following result is returned:

```
BRAND SIZE sp
"", 0.0, 32936  (the total price of all mobile phones)
"", 1.4, 169      (prices for different phone sizes)
"", 4.7, 7897
"", 5.0, 899
"", 5.5, 14684
"", 5.6, 5688
"", 5.9, 3599
"Huawei", 0, 7987   (prices for different brands)
"Meizu", 0, 1299
"Nokia", 0, 169
"Xiaomi", 0, 899
"OPPO", 0, 2999
"Samsung", 0, 5688
"Apple", 0, 13895
```
If prices of mobile phones are calculated by size, the brand column becomes invalid. If prices of mobile phones are calculated by brand, the size column is invalid. If the total price of all mobile phones is calculated, the brand and size columns become invalid. In the preceding result set, each null value is replaced with the default value.

If the system replaces each invalid value with the default value or null value, you may need to distinguish between the default value or null value from the existing value in the original data set. For example, if the value for a brand in the data set is "" that indicates an empty string, you may need to distinguish the empty string from invalid brand values that the system generates when you aggregate data by size.

In this case, you can change the preceding SQL statement to the following SQL statement that uses the GROUPING function:

```
SELECT brand,size,SUM(price) AS sp, GROUPING(brand,size),GROUPING(brand) as g1
FROM phone
GROUP BY GROUPING SETS ((brand),(size),()) LIMIT 20
```

Result

```
brand size sp GROUPING(brand,size) g1
"", 0.0, 32936, 3, 1
"", 1.4, 169, 2, 1
"", 4.7, 7897, 2, 1
"", 5.0, 899, 2, 1
"", 5.5, 14684, 2, 1
"", 5.6, 5688, 2, 1
"", 5.9, 3599, 2, 1
"Huawei", 0, 7987, 1, 0
"Meizu", 0, 1299, 1, 0
"Nokia", 0, 169, 1, 0
"Xiaomi", 0, 899, 1, 0
"OPPO", 0, 2999, 1, 0
"Samsung", 0, 5688, 1, 0
"Apple", 0, 13895, 1, 0
```

As a result, the grouping(brand,size) and grouping(brand) as g1 columns are added.

The GROUPING function is used for the columns. The following section describes the GROUPING function:

You can include up to 32 parameters in the GROUPING function. The output of the function is an integer. Each bit value in the integer in binary corresponds to an input parameter of the function. If the result includes the column of an input parameter, the bit value is 0. If the result does not include the column of an input parameter, the bit value is 1.

In the preceding example, in the second to eighth rows of the result for GROUPING(brand), each value of the brand parameter is the default value. These values are null values. The empty string in the actual data set is not used as each value of the brand parameter because mobile phones are calculated by size. The result of GROUPING(brand) is 1. This rule also takes effect on the data from the ninth row to the fifteenth row. The result for queries on data from the ninth row to the fifteenth row is 0.

In the second to eighth rows, the values of the size field are obtained from the actual data set. The low bit value of GROUPING(brand,size) is 0 and the high bit value is 1. The result of GROUPING(brand,size) is 2.