---
toc: content
order: 30316
---

# UNION
## 描述
用来将多路相同schema的数据做合并。要求两边的字段完全一致，包括字段类型、字段顺序。

UNION ALL 会保留重复值，而UNION 会进行去重处理，等价于UNION ALL + DISTINCT，运行效率较低，不建议使用。

多路UNION时，不支持UNION和UNION ALL混用；

## 语法格式

```
select_statement
	UNION [ALL]
	select_statement
   [UNION [ALL]
	select_statement];
```

## 示例
1. 双路合并

```
SELECT nid, brand, price, size FROM phone WHERE nid < 5
UNION ALL
SELECT nid, brand, price, size FROM phone WHERE nid > 5
```

2. 多路合并

```
SELECT 
    nid, brand, price, size 
FROM 
    phone 
WHERE nid >= 3 AND nid <= 5 
UNION ALL 
SELECT 
    nid, brand, price, size 
FROM 
    phone 
WHERE nid >= 6 AND nid <= 10 
UNION ALL 
SELECT nid, brand, price, size 
FROM 
    phone 
WHERE nid >= 30 AND nid <= 40
```