### Left Outer Join
* 左连接。返回左表中的所有记录，即使右表中没有与之匹配的记录。
```
SELECT
  t1.id, t2.id
FROM
  tj_shop AS t1
LEFT JOIN
  tj_item AS t2
ON
  t1.id = t2.id
```

### Inner Join
* 内连接。关键字inner可以省略。返回满足ON条件的行；
```
SELECT
  t1.id, t2.id
FROM
  tj_shop AS t1
JOIN
  tj_item AS t2
ON
  t1.id = t2.id
```

* 支持ON条件恒为TRUE，此时返回两表的笛卡尔积；下述两条SQL等价；
```
SELECT
  t1.id, t2.id
FROM
  tj_shop AS t1
JOIN
  tj_item AS t2
ON
  TRUE

SELECT
  t1.id, t2.id
FROM
  tj_shop, tj_item;
```

### Semi Join
* 半连接，SEMI JOIN中，右表只用于过滤左表的数据而不出现在结果集中。
* 当join条件成立时，返回左表中的数据。如果tj_shop中某行的id在tj_item的所有id中出现过，则此行保留在结果集中。

```
SELECT
  id
FROM
  tj_shop
WHERE id IN (
  SELECT 
    id 
  FROM 
    tj_item
)
```

```
SELECT
  id
FROM
  tj_shop
WHERE EXISTS (
  SELECT 
    id 
  FROM 
    tj_item
  WHERE
    tj_shop.id = id
)
```

### Anti Join
* 反连接，Anti JOIN中，右表只用于过滤左表的数据而不出现在结果集中。
* 当join条件不成立时，返回左表中的数据。如果tj_shop中某行的id在tj_item的所有id中不存在，则此行保留在结果集中。

```
SELECT
  id
FROM
  tj_shop
WHERE id NOT IN (
  SELECT 
    id 
  FROM 
    tj_item
)
```

```
SELECT
  id
FROM
  tj_shop
WHERE NOT EXISTS (
  SELECT 
    id 
  FROM 
    tj_item
  WHERE
    tj_shop.id = id
)
```