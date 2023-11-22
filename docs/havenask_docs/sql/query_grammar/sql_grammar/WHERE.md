---
toc: content
order: 30313
---

# WHERE
## 描述
WHERE用于有条件地从表中选取数据。

## 语法格式

```
select:
  SELECT [ DISTINCT ]
    { * | projectItem [, projectItem ]* }
  FROM tableExpression
    [ WHERE booleanExpression ]
```

其中booleanExpression可以为以下几种：

|序号|表达式类型|实例|
|---|---|---|
|0|AND, OR|WHERE a > 1 AND a < 100, WHERE a > 5 OR c > 100|
|1| >, >=, <, <=, <>||
|2|IN|WHERE id IN (1, 2, 3, 4, 5)|

## 示例

```
SELECT * FROM table WHERE f1 > 10 AND f2 < 5

SELECT * FROM table WHERE id IN (5, 6, 7, 8, 9)
```
## Hint
sql支持where使用havenask的倒排优化加速查找，如MATCHINDEX，QUERY为兼容havenask查询的语法实现，以及等值条件，如 `SELECT * FROM table WHERE f1 = 10`，当f1是建立倒排索引的等值条件场景下，能够自动优化为倒排查找。scan op会自动提取能够优化的query 条件。


