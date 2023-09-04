## 描述
Group By用来对一个或者多个字段聚合，以此算出相应的数据，比如最大值、最小值、平均值等。

## 语法格式
```
select:
    SELECT [ ALL | DISTINCT ]
    { * | projectItem [, projectItem ]* }
    FROM tableExpression
    [ WHERE booleanExpression ]
    [ GROUP BY { groupItem [, groupItem ]* } ]
    [ HAVING booleanExpression ]
```

## 内置UDAF函数
目前内置的UDAF如下：

|序号 | 函数名 | 功能|
-- | -- | --|
|1 | SUM | 聚合后求和|
|2 | MIN | 聚合后求最小值|
|3 | MAX | 聚合后求最大值|
|4 | COUNT | 统计数据条数|
|5 | AVG | 聚合后计算平均值|
|6 | ARBITRARY​ | 任意选择一个输入值作为结果输出|

## 示例
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

6. having子句

```
SELECT brand FROM phone GROUP BY brand HAVING COUNT(brand) > 10
```