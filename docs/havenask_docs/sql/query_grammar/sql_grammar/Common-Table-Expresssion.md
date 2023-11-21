---
toc: content
order: 30333
---

# Common-Table-Expression
## 简介
CTE 全称 Common Table Expresssion, 我们可以通过CTE定义一个临时（query生存期）的命名结果集，在query接下来的查询语句（不局限于SELECT，但是我们目前只支持了SELECT）中就可以直接引用这个CTE，从而达到简化查询的目的。

一个标准CTE定义语法如下所示：

```
WITH expression_name[(column_name [,...])]
AS
   (CTE_definition)
SQL_statement;
```

首先，我们需要指定这个CTE的name，以方便在后面的引用。

然后需要在括号中定义输出列名，如果有多个输出列名，则用逗号分隔开，这里的列的数目和类型要和CTE定义子句中的输出列数目和类型对齐。此处定义好的列名即为最后的输出列名，如果这里没有定义输出列名，则以CTE定义子句中实际输出的列为准。

随后通过一个AS来定义CTE的数据产出子句，改CTE最后产出的数据将由这个子句产生。

OK，到这里一个完整的CTE已经定义完成，接下来就可以在SQL语句中直接引用它啦。

## 示例

接下来通过几个具体的例子来展示如何使用CTE

```
WITH T_CTE (i1_cte,i2_cte) AS (SELECT i1,i2 FROM t1)
SELECT * FROM T_CTE
```

上图是一个最简单的CTE，在SELECT语句中直接输出了CTE定义的结果集

```
WITH T_CTE (i1_cte,i2_cte) AS (SELECT i1,i2 FROM t1)
SELECT * FROM t2 JOIN T_CTE ON t2.i1 = T_CTE.i1_cte AND t2.i2 = T_CTE.i2_cte
```

你也可以在JOIN中去使用他，

```
WITH T_CTE (i1_cte,i2_cte) AS (SELECT i1,i2 FROM t1)
SELECT * FROM t2 WHERE EXISTS (SELECT * FROM T_CTE WHERE t2.i1 = i1_cte AND t2.i2 = i2_cte)
```

或者在子查询中去使用

## FAQ
### Performence

虽然在上文中提到，CTE是定义了一个临时结果集，但是真正的执行顺序并非是先产出CTE的结果集再在select中去加工，实际上，CTE的定义语句会在生成执行计划的阶段被展开，从而和引用者select一同被优化。所以执行计划并不会比不用CTE时候复杂，两者具有同等的性能表现。
