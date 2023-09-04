<a name="Eoesh"></a>
UDF 使用介绍
<a name="GHATz"></a>
## 内置UDF列表
<a name="lvxHf"></a>

<a name="ad3GU"></a>
#### 复杂类型函数
| 函数名 | 功能简介 | 版本 |
| --- | --- | --- |
| [contain](#contain) | 判断字段值在给定集合内，支持单值和多值 | ALL |
| [notcontain](#notcontain) | 判断字段值不在给定集合内，支持单值和多值 | ALL |
| [MATCHINDEX](#MATCHINDEX) | 使用给定的条件查询指定字段的倒排索引 | ALL |
| [QUERY](#QUERY) | 使用给定的条件查询倒排索引, 原HA3 query语法 | ALL |
| [hashcombine](#hashcombine) | 将多个int64的值合并成1个int64的值 | ALL |
| [rangevalue](#rangevalue) | 按范围将字段值做映射 | ALL |
| [sphere_distance](#sphere_distance) | 计算经纬度距离 | ALL |
| [ha_in](#ha_in) | 判断字段值在给定集合内 | ALL |


<a name="CAGWf"></a>
## 使用示例

### 检索示例

- 检索全表内容

```sql
SELECT nid, price, brand, size FROM phone ORDER BY nid LIMIT 1000 
```

```
USE_TIME: 0.881, ROW_COUNT: 10

------------------------------- TABLE INFO ---------------------------
                 nid |               price |               brand |                size |
                   1 |                3599 |              Huawei |                 5.9 |
                   2 |                4388 |              Huawei |                 5.5 |
                   3 |                 899 |              Xiaomi |                   5 |
                   4 |                2999 |                OPPO |                 5.5 |
                   5 |                1299 |               Meizu |                 5.5 |
                   6 |                 169 |               Nokia |                 1.4 |
                   7 |                3599 |               Apple |                 4.7 |
                   8 |                5998 |               Apple |                 5.5 |
                   9 |                4298 |               Apple |                 4.7 |
                  10 |                5688 |             Samsung |                 5.6 |
```

<a name="vvd3K"></a>
#### contain

- 原型
```
boolean contain(int32 a, const string b)
boolean contain(int64 a, const string b)
boolean contain(string a, const string b)
boolean contain(ARRAY<int32> a, const string b)
boolean contain(ARRAY<int64> a, const string b)
boolean contain(ARRAY<string> a, const string b)
```

- 说明

判断单值或多值a中是否包含b中描述的内容

- 参数

参数a：输入为单值多值的int32/int64/string 类型<br />参数b：输入为常量string表达式，用 `|` 分隔，表示满足任意一项即可

- 返回值

boolean类型返回，表示参数a是否包含参数b中描述的集合

- 示例

使用 `contain` ，检索nid字段值在[1,2,3]的所有记录

```sql
SELECT nid, price, brand, size FROM phone WHERE contain(nid, '1|2|3') ORDER BY nid LIMIT 100
```

```
USE_TIME: 0.059, ROW_COUNT: 3

------------------------------- TABLE INFO ---------------------------
                 nid |               price |               brand |                size |
                   1 |                3599 |              Huawei |                 5.9 |
                   2 |                4388 |              Huawei |                 5.5 |
                   3 |                 899 |              Xiaomi |                   5 |
```

<a name="WwA54"></a>
#### notcontain

- 原型
```
boolean notcontain(int32 a, const string b)
boolean notcontain(int64 a, const string b)
boolean notcontain(string a, const string b)
boolean notcontain(ARRAY<int32> a, const string b)
boolean notcontain(ARRAY<int64> a, const string b)
boolean notcontain(ARRAY<string> a, const string b)
```

- 说明

判断单值或多值a中是否**不在**b中描述的内容

- 参数

参数a：输入为单值多值的int32/int64/string 类型<br />参数b：输入为常量string表达式，用 `|` 分隔，表示不能满足任意一项

- 返回值

boolean类型返回，表示参数a是否**不在**参数b中描述的集合

- 示例

使用 `notcontain` ，检索nid字段值**不在**[1,2,3]范围内的所有记录

```sql
SELECT nid, price, brand, size FROM phone WHERE notcontain(nid, '1|2|3') ORDER BY nid LIMIT 100
```

```
USE_TIME: 0.092, ROW_COUNT: 7

------------------------------- TABLE INFO ---------------------------
                 nid |               price |               brand |                size |
                   4 |                2999 |                OPPO |                 5.5 |
                   5 |                1299 |               Meizu |                 5.5 |
                   6 |                 169 |               Nokia |                 1.4 |
                   7 |                3599 |               Apple |                 4.7 |
                   8 |                5998 |               Apple |                 5.5 |
                   9 |                4298 |               Apple |                 4.7 |
                  10 |                5688 |             Samsung |                 5.6 |
```

<a name="m4OVw"></a>
#### MATCHINDEX

- 原型
```
boolean MATCHINDEX(const string a, const string b)
boolean MATCHINDEX(const string a, const string b, const string c)
```

- 说明

判断字段a中是否**包含**b中描述的内容，单字段索引的召回<br />仅限索引表召回阶段倒排加速优化使用，用于where条件中

- 参数

参数a：输入为常量string 类型，对应建立倒排优化字段<br />
参数b：输入为常量string 类型，内容为字符串描述内容<br />
参数c：可选项，可描述内容包含分词器，停用词等，用于b中内容的分词。主要通过下以关键词设置，使用`:` `,` 分隔每一项。

      - global_analyzer
      - specific_index_analyzer
      - no_token_indexes
      - tokenize_query
      - remove_stopwords
      - default_op
- 返回值

boolean类型返回，表示字段a中**是否含有**参数b中描述的内容

- 示例

使用 `MATCHINDEX` ，检索倒排字段 `title` 中含有"镜头"关键字的记录

```sql
SELECT nid, brand FROM phone WHERE MATCHINDEX('title', '镜头')
```

```
------------------------------- TABLE INFO ---------------------------
                 nid |               brand |
                   1 |              Huawei |
```

<a name="JZXMG"></a>
#### QUERY

- 原型
```
boolean QUERY(const string a, const string b)
boolean QUERY(const string a, const string b, const string c)
```

- 说明

判断字段a中是否**包含**b中描述的内容，提供自动分词并检索能力，<br />用于支持在SQL模式下使用HA3引擎原生的查询语法[ha3 query语法](./ha3_query_clause.md)，<br />仅限索引表召回阶段倒排加速优化使用，用于where条件中

- 参数

参数a：输入为常量string 类型，会作为默认索引字段<br />
参数b：输入为常量string 类型，内容为字符串描述内容<br />
参数c：可选项，可描述内容包含分词器，停用词等，用于b中内容的分词，使用`:` `,` 分隔每一项，主要通过下以关键词设置：

      - global_analyzer
      - specific_index_analyzer
      - no_token_indexes
      - tokenize_query
      - remove_stopwords
      - default_op
- 返回值

boolean类型返回，表示字段a中**是否含有**参数b中描述的内容

- 示例
   - 使用 `QUERY` ，查询 `title` 中含有"Huawei手机"的条目。
```sql
SELECT nid, price, brand, size FROM phone WHERE QUERY(title, 'Huawei手机')
```

```
USE_TIME: 0.034, ROW_COUNT: 1

------------------------------- TABLE INFO ---------------------------
                 nid |               price |               brand |                size |
                   2 |                4388 |              Huawei |                 5.5 |
```


   - 使用组合条件，检索`title`中含有 "Huawei手机" 或者 "OPPO手机" 条目

```sql
SELECT nid, price, brand, size FROM phone 
   WHERE QUERY(title, 'Huawei手机 OR OPPO手机')
```

```
USE_TIME: 0.03, ROW_COUNT: 2

------------------------------- TABLE INFO ---------------------------
                 nid |               price |               brand |                size |
                   2 |                4388 |              Huawei |                 5.5 |
                   4 |                2999 |                OPPO |                 5.5 |
```

  

   - 指定default_op等用法，使用`:` `,` 分隔
```sql
SELECT nid, price, brand, size FROM phone 
   WHERE QUERY(title, 'Huawei手机 OPPO手机', 'default_op:OR,remove_stopwords:true')
```

- 备注

QUERY udf的参数2是通过HA3 query语法解析器解析的，HA3 query语法可以参考 [query子句](./ha3_query_clause.md)] 一节。当参数2填入的是常量字符串，代入HA3 query需要注意将前后单引号去掉，如QUERY(title, 'Huawei手机 OPPO手机')等价于HA3查询query=Huawei手机 OPPO手机。如果需要在query描述内部加引号，如形为query='Huawei手机' AND 'OPPO手机' 的HA3查询串，QUERY udf中的等价形式为 QUERY(title, '''Huawei手机'' AND ''OPPO手机''')。

- 典型错误
| 错误类型 | 错误形式 | 正确形式 |
| --- | --- | --- |
| 语法报错，查询无结果 | QUERY('pidvid','123:456') | QUERY('pidvid','"123:456"') |

<a name="VQz4F"></a>
#### 
<a name="UMfpo"></a>
#### hashcombine

- 原型
```
int64 hashcombine(int64 k1, int64 k2, ...)
```

- 说明

将多个int64整数通过内置hash函数合并成1个int64

- 参数

多个int64的整数列

- 返回值

hash后的值

- 示例

使用`hashcombine`，将cate_id和user_id合并成一个值
```sql
SELECT hashcombine(cate_id,user_id) FROM phone;
```
<a name="jZFoo"></a>
#### rangevalue

- 原型
```
float rangevalue(float v, string desc)
```

- 说明

将连续值映射成离散值

- 参数

参数v：连续值的列<br />参数desc：映射规则

- 返回值

映射后的离散值

- 示例

使用`rangevalue`，对price的值做映射：小于等于1000映射为1.0，大于1000小于等于5000映射为2.0，其他保留原始价格值
```sql
SELECT rangevalue(price,'(,1000]:1.0;(1000,5000]:2.0') FROM phone;
```
<a name="ysbYg"></a>
#### sphere_distance

- 原型
```
double sphere_distance(LOCATION point, double longitude, double latitude)
```

- 说明

计算输入的经纬度和检索文档的距离

- 参数

参数point：LOCATION类型单值的列<br />参数longitude：经度<br />参数latitude：纬度

- 返回值

地球球面距离，单位为千米

- 示例

使用`sphere_distance`。geo必须是LOCATION类型单值字段且创建了正排
```sql
SELECT sphere_distance(geo,127.0,30.0) FROM phone
```

<a name="sCuT0"></a>
#### ha_in

- 原型
```
boolean ha_in(int32 v, const string desc, [const string sep])
boolean ha_in(ARRAY<int32> v, const string desc, [const string sep])
boolean ha_in(int64 v, const string desc, [const string sep])
boolean ha_in(ARRAY<int64> v, const string desc, [const string sep])
boolean ha_in(string v, const string desc, [const string sep])
boolean ha_in(ARRAY<string> v, const string desc, [const string sep])
```

- 说明

类似contain的用法，判断描述内容是否存在于v字段中，可以用于[summary](./Summary查询.md)/[kv/kkv](./KV、KVV查询.md)表查询

- 参数

参数v：字段，支持单值多值类型<br />参数desc：常量描述，可表述多个项，v满足其中一项即可<br />参数sep：可选项，分隔符，拆个参数desc内容成多项，默认`|`

- 返回值

判断v是否满足desc描述

- 示例

使用`ha_in`，检索nid字段值在描述里的所有记录
```sql
select nid from mainse_excellent_search where ha_in(nid, '641999912989|577150359640|123456')
```
```
USE_TIME: 32.141ms, ROW_COUNT: 2

------------------------------- TABLE INFO ---------------------------
                  nid(int64) |
                641999912989 |
                577150359640 |
```


<a name="uu7k2"></a>
## 附录：
<a name="U3sqX"></a>
### 
<a name="ORuoy"></a>
### 分词器描述：
用于MATCHINDEX，QUERY等使用的参数说明

- [global_analyzer]()：

查询中指定全局的分词器，该分词器会覆盖schema的分词器，指定的值必须在analyzer.json里有配置

- specific_index_analyzer：查询中指定index使用另外的分词器，该分词器会覆盖global_analyzer和schema的分词器

- no_token_indexes：支持查询中指定的index不分词（除分词以外的其他流程如归一化、去停用词会正常执行），<br />多个index之间用`;`分割

- tokenize_query：`true` or `false` 表示是否需要对query进行分词，`QUERY` UDF带了分词器该参数不生效<br />默认`true`

- remove_stopwords：`true` or `false` 表示是否需要删除stop words，stop words在分词器中配置<br />默认`true`

- default_op：指定在该次查询中使用的默认query 分词后的连接操作符，`AND` or `OR`，默认为AND。
