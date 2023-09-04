## 描述
用于对一个或者多个字段进行排序。默认为升序(ASC)。由于排序性能较差，要求排序时必须加上LIMIT子句。

## 语法格式

```
select:
  SELECT [ DISTINCT ]
    { projectItem [, projectItem ]* }
  FROM tableExpression
    ORDER BY { orderByItem [ASC|DESC] [,OrderByItem ASC|DESC]* }
    LIMIT N
    OFFSET M
```

## 示例
1. 简单排序

```
SELECT nid, brand, price, size FROM phone ORDER BY price LIMIT 1000
```

2. 带升降排序标志的排序

```
SELECT nid, brand, price, size FROM phone ORDER BY price ASC LIMIT 1000
```

3. 多字段排序

```
SELECT nid, brand, price, size FROM phone ORDER BY size DESC, price DESC LIMIT 1000
```

4. 返回价格排序后第11到第20名的结果

```
SELECT nid, brand, price, size FROM phone ORDER BY price DESC LIMIT 10 OFFSET 10
```

5. 不排序，随机返回10个商品

```
SELECT nid, brand, price, size FROM phone LIMIT 10
```