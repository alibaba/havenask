---
toc: content
order: 30315
---

# ORDER BY
## Description
You can use ORDER BY to sort one or more fields. The default value is ASC. ASC specifies the ascending order. When you sort data, you must use a LIMIT clause to improve the sorting performance.

## Syntax

```
select:
  SELECT [ DISTINCT ]
    { projectItem [, projectItem ]* }
  FROM tableExpression
    ORDER BY { orderByItem [ASC|DESC] [,OrderByItem ASC|DESC]* }
    LIMIT N
    OFFSET M
```

## Examples
1. Sort data in a simple manner.

```
SELECT nid, brand, price, size FROM phone ORDER BY price LIMIT 1000
```

2. Sort data in ascending order or descending order.

```
SELECT nid, brand, price, size FROM phone ORDER BY price ASC LIMIT 1000
```

3. Sort multiple fields.

```
SELECT nid, brand, price, size FROM phone ORDER BY size DESC, price DESC LIMIT 1000
```

4. Return the records of the eleventh to twentieth prices in the price sorting result.

```
SELECT nid, brand, price, size FROM phone ORDER BY price DESC LIMIT 10 OFFSET 10
```

5. Return the data of 10 products randomly without sorting the data.

```
SELECT nid, brand, price, size FROM phone LIMIT 10
```
