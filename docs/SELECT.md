## 描述
SELECT 语句用于从表中选取数据。

SELECT语句是查询语法的核心，所有高级查询语法都是围绕SELECT语句展开的。本节将简单介绍havenask支持的SELECT语句语法，高级用法请参考其他的小节。

## 语法格式
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

## 示例
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