<a name="ZvNC4"></a>
## 内置UDAF列表

havenask目前内置了几种常见UDAF：

- sum：聚合后求和
- avg：聚合后求均值
- max：聚合后求最大值
- min：聚合后求最小值
- count：聚合后统计条目数
- ARBITRARY：聚合后选择某一个值（一般用于从“值全部相同的字段”中返回数据），其它SQL引擎中可能称为IDENTITY函数
- GATHER：将多个单值聚合为一个多值
- MULTIGATHER：将多个多值聚合为一个多值
- MAXLABEL：聚合后求最大值对应的Label值

<a name="X9ytW"></a>
## 使用示例


<a name="i67jR"></a>
### 测试数据

后续演示将使用测试环境的 `phone` 表进行，表中主要记录了主流品牌的手机信息，表的内容如下：

| nid | title | price | brand | size | color |
| --- | --- | --- | --- | --- | --- |
| 1 | 华为 Mate 9 麒麟960芯片 徕卡双镜头 | 3599 | 华为 | 5.9 | 红 |
| 2 | Huawei/华为 P10 Plus全网通手机 | 4388 | 华为 | 5.5 | 蓝 |
| 3 | Xiaomi/小米 红米手机4X 32G全网通4G智能手机 | 899 | 小米 | 5.0 | 黑 |
| 4 | OPPO R11 全网通前后2000万指纹识别拍照手机r11r9s | 2999 | OPPO | 5.5 | 红 |
| 5 | Meizu/魅族 魅蓝E2 全网通正面指纹快充4G智能手机 | 1299 | Meizu | 5.5 | 银白 |
| 6 | Nokia/诺基亚 105移动大声老人机直板按键学生老年小手机超长待机 | 169 | Nokia | 1.4 | 蓝 |
| 7 | Apple/苹果 iPhone 6s 32G 原封国行现货速发 | 3599 | Apple | 4.7 | 银白 |
| 8 | Apple/苹果 iPhone 7 Plus 128G 全网通4G手机 | 5998 | Apple | 5.5 | 亮黑 |
| 9 | Apple/苹果 iPhone 7 32G 全网通4G智能手机 | 4298 | Apple | 4.7 | 黑 |
| 10 | Samsung/三星 GALAXY S8 SM-G9500 全网通 4G手机 | 5688 | Samsung | 5.6 | 雾屿蓝 |


<a name="AttrF"></a>
### 检索示例

- 检索全表内容

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

注：title和color是Summary字段，所以在本阶段查询中显示为null

- 使用sum函数统计每个品牌商品的总价
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

- 使用avg函数统计每个品牌最贵手机的价格，并按照最高价格降序排列

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


- 使用MAXLABEL函数统计每个品牌最贵手机的屏幕尺寸
```sql
SELECT brand, MAXLABEL(size, price) AS size FROM phone GROUP BY brand
```

