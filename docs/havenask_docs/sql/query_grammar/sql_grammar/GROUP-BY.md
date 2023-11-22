---
toc: content
order: 30314
---

# GROUP BY
## Group-BY
Group By用来对一个或者多个字段聚合，以此算出相应的数据，比如最大值、最小值、平均值等。

### 语法格式
```
select:
    SELECT [ ALL | DISTINCT ]
    { * | projectItem [, projectItem ]* }
    FROM tableExpression
    [ WHERE booleanExpression ]
    [ GROUP BY { groupItem [, groupItem ]* } ]
    [ HAVING booleanExpression ]
```

### 内置UDAF函数
目前内置的UDAF如下：

|序号 | 函数名 | 功能|
-- | -- | --|
|1 | SUM | 聚合后求和|
|2 | MIN | 聚合后求最小值|
|3 | MAX | 聚合后求最大值|
|4 | COUNT | 统计数据条数|
|5 | AVG | 聚合后计算平均值|
|6 | ARBITRARY​ | 任意选择一个输入值作为结果输出|

### 示例
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

## GROUPING-SET
Grouping Sets是对SQL中的Group By语句的扩展。允许用户采用多种方式对结果分组，而不必使用多个SELECT语句来实现这一目的。这样能够使引擎给出更简练的执行计划，从而提高执行性能。

### 示例
```
SELECT brand,size,SUM(price) AS sp 
FROM phone 
GROUP BY GROUPING SETS ((brand),(size),())
```

如上所示，是grouping sets的典型使用场景，我们想对手机表分别根据品牌和尺寸统计其价格，并统计整个手机表所有手机的价格，这样我们的group by就有三个子集brand（根据品牌统计），size（根据尺寸统计），(空）（统计所有手机价格）。最后的输出如下：

```
BRAND SIZE sp
"", 0.0, 32936  （所有手机总价）
"", 1.4, 169     （各种尺寸的价格统计）
"", 4.7, 7897
"", 5.0, 899
"", 5.5, 14684
"", 5.6, 5688
"", 5.9, 3599
"Huawei", 0, 7987   （各种品牌的价格统计）
"Meizu", 0, 1299
"Nokia", 0, 169
"Xiaomi", 0, 899
"OPPO", 0, 2999
"Samsung", 0, 5688
"Apple", 0, 13895
```
OK, 我们很容易发现了问题，当以尺寸统计的时候，brand这一列是无效的，当以品牌统计的时候，size这一列是无效的，而统计手机总价的时候，brand 和 size都是无效的，在上面的结果集中，我们使用了默认值来替代本该出现的null值。

那么另一个问题随之而来，当我们在无效值的位置上填上了默认值（或者null值，后面叙述中默认值包含null值的含义）的时候，在这个结果集上如何将其和原始数据集中就有的值区分开呢，比如如果我们的数据集中就有一个品牌是 ""（空字符串），那么如何将它和因为根据size汇总而导致的无效的brand值区分开呢？

我们提供了grouping函数搭配使用，当用上grouping函数的时候，上面的sql就变成了下面的形式

```
SELECT brand,size,SUM(price) AS sp, GROUPING(brand,size),GROUPING(brand) as g1 
FROM phone 
GROUP BY GROUPING SETS ((brand),(size),()) LIMIT 20
```

结果

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

多了两列，grouping(brand,size) 和grouping(brand) as g1。

这两列都用到了GROUPING函数，GROUPING函数的解释如下：

grouping函数接受多个入参（当前最多32个），其输出是一个整数值，整数值的二进制形式中的每一位(bit)都对应函数的入参，当入参的列在结果列中出现时，该位为0，否则为1。

比如在上面的例子中，GROUPING(brand),在结果的2-8行中，因为是按照size进行统计的，所以2-8行的brand实际上是填的默认值（空值）而不是来自于实际数据集中的空串（空值），所以GROUPING(brand)结果为1。同理，9-15行结果为0。

而2-8行的时候，size是来自于实际的数据集，所以GROUPING(brand,size)的低位为0，高位为1，最后结果就是2。