---
toc: content
order: 3
---

# UDAF introduction
<a name="ZvNC4"></a>
##
<a name="dhMIP"></a>
## Description
<a name="SHfZV"></a>
##
In the aggregation operation of Havenask SQL, you can use UDAFs (User Defined Aggregation Function) to collect statistics on data entries based on the aggregated group. Havenask SQL provides some built-in UDAFs. You can also use plug-ins to develop your own UDAFs.

<a name="ZvNC4"></a>
## Built-in UDAFs

Havenask provides several common built-in UDAFs:

- SUM: aggregates intermediate values and returns the sum of the values.
- AVG: aggregates intermediate values and returns the average of the values.
- MAX: aggregates intermediate values and returns the maximum value.
- MIN: aggregates intermediate values and returns the minimum value.
- COUNT: aggregates intermediate values and returns the number of entries.
- ARBITRARY: aggregates intermediate values and returns a random value. In most cases, this function is used to return the value of a field that contains the same value. This function is used in the same manner as the IDENTITY function in other SQL engines.
- GATHER: aggregates multiple single values into one value.
- MULTIGATHER: aggregates multiple multivalued values into one value.
- MAXLABEL: aggregates intermediate values and returns the maximum value of the specified label.

<a name="X9ytW"></a>
## Sample code


<a name="i67jR"></a>
### Test data

Subsequent demonstrations will be conducted using the `phone` table in the test environment. The table mainly records the mobile phone information of mainstream brands. The content of the table is as follows:

| nid | title | price | brand | size | color |
| --- | --- | --- | --- | --- | --- |
| 1 | Huawei Mate 9 Kirin 960 Chip Leica Dual Lens | 3599 | Huawei | 5.9 | Red |
| 2 | Huawei P10 Plus Unlocked Mobile Phone | 4388 | Huawei | 5.5 | Blue |
| 3 | Xiaomi Redmi Mobile Phone 4X 32 GB Unlocked 4G Smartphone | 899 | Xiaomi | 5.0 | Black |
| 4 | OPPO R11 Unlocked 20 Megapixel Front and Rear Cameras Fingerprint Identification Camera Phone r11r9s | 2999 | OPPO | 5.5 | Red |
| 5 | MEIZU Meilan E2 Unlocked Front Fingerprint Fast Charging 4G Smartphone | 1299 | Meizu | 5.5 | Silvery white |
| 6 | Nokia 105 Mobile Loud Phone for Seniors Straight Button Students Old People Small Mobile Phone Super Long Standby | 169 | Nokia | 1.4 | Blue |
| 7 | Apple iPhone 6s 32 GB Unbroken Seal Genuine Spot Goods Quick Delivery | 3599 | Apple | 4.7 | Silvery white |
| 8 | Apple iPhone 7 Plus 128 GB Unlocked 4G Mobile Phone | 5998 | Apple | 5.5 | Bright black |
| 9 | Apple iPhone 7 32 GB Unlocked 4G Smartphone | 4298 | Apple | 4.7 | Black |
| 10 | Samsung GALAXY S8 SM-G9500 Unlocked 4G Mobile Phone | 5688 | Samsung | 5.6 | Fog blue |


<a name="AttrF"></a>
### Sample queries

- Query full data in the phone table.

```sql
SELECT * FROM phone ORDER BY nid LIMIT 1000
```

```
USE_TIME: 0.036, ROW_COUNT: 10

------------------------------- TABLE INFO ---------------------------
                 nid |               title |               price |               brand |                size |               color |
                   1 |                null |                3599 |              Huawei |                 5.9 |                null |
                   2 |                null |                4388 |              Huawei |                 5.5 |                null |
                   3 |                null |                 899 |              Xiaomi |                   5 |                null |
                   4 |                null |                2999 |                OPPO |                 5.5 |                null |
                   5 |                null |                1299 |               Meizu |                 5.5 |                null |
                   6 |                null |                 169 |               Nokia |                 1.4 |                null |
                   7 |                null |                3599 |               Apple |                 4.7 |                null |
                   8 |                null |                5998 |               Apple |                 5.5 |                null |
                   9 |                null |                4298 |               Apple |                 4.7 |                null |
                  10 |                null |                5688 |             Samsung |                 5.6 |                null |
```

Note: The system returns null as the values of these fields after the first phase of the query is complete because the title and color fields are included in the summary.

- Use the SUM function to calculate the total price of the commodities of each brand.
```sql
SELECT brand, sum(price) FROM phone GROUP BY (brand) ORDER BY brand LIMIT 1000
```

```
USE_TIME: 0.152, ROW_COUNT: 7

------------------------------- TABLE INFO ---------------------------
               brand |          SUM(price) |
               Apple |               13895 |
              Huawei |                7987 |
               Meizu |                1299 |
               Nokia |                 169 |
                OPPO |                2999 |
             Samsung |                5688 |
              Xiaomi |                 899 |
```

- Use the MAX function to obtain the highest prices of the mobile phones of each brand and rank the prices in descending order

```sql
SELECT brand, max(price) AS price FROM phone GROUP BY (brand) ORDER BY price DESC LIMIT 1000
```

```
USE_TIME: 0.053, ROW_COUNT: 7

------------------------------- TABLE INFO ---------------------------
               brand |               price |
               Apple |                5998 |
             Samsung |                5688 |
              Huawei |                4388 |
                OPPO |                2999 |
               Meizu |                1299 |
              Xiaomi |                 899 |
               Nokia |                 169 |
```


- Use the MAXLABEL function to obtain the screen size of the most expensive mobile phone from each brand
```sql
SELECT brand, MAXLABEL(size, price) AS size FROM phone GROUP BY brand
```

