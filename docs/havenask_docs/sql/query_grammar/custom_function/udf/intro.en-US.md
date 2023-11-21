---
toc: content
order: 1
---

# UDF introduction
## Description

Havenask SQL supports the use of built-in UDFs (User Defined Function) in query statements and allows customers to develop their own UDFs in the form of plug-ins.

<a name="Eoesh"></a>
Introduction to UDF usage
<a name="GHATz"></a>
## Built-in UDFs
<a name="lvxHf"></a>

<a name="ad3GU"></a>
#### Complex type functions
| The name of the Function Compute function. | Description | Edition and version |
| --- | --- | --- |
| [contain](#contain) | Returns true if a specific value in the specified field is included in the specified value set. The field can be a single-value field or multi-value field. | ALL |
| [notcontain](#notcontain) | Returns true if a specific value in the specified field is excluded from the specified value set. The field can be a single-value field or multi-value field. | ALL |
| [MATCHINDEX](#MATCHINDEX) | Returns true if the inverted index of the specified field meets the specified conditions. | ALL |
| [QUERY](#QUERY) | Returns inverted indexes based on the specified conditions. The syntax of this function is the same as the syntax of the query clause supported by HA3. | ALL |
| [hashcombine](#hashcombine) | Merges values in multiple fields of the INT64 type into one value of the INT64 type. | ALL |
| [rangevalue](#rangevalue) | Converts values in the specified ranges in a field to values of the specified data type. | ALL |
| [sphere_distance](#sphere_distance) | Calculates a distance value based on the specified longitude and latitude. | ALL |
| [ha_in](#ha_in) | Returns true if a specific value in the specified field is included in the specified value set. | ALL |


<a name="CAGWf"></a>
## Sample code

#### contain

- Prototype
```
boolean contain(int32 a, const string b)
boolean contain(int64 a, const string b)
boolean contain(string a, const string b)
boolean contain(ARRAY<int32> a, const string b)
boolean contain(ARRAY<int64> a, const string b)
boolean contain(ARRAY<string> a, const string b)
```

- Descriptions

You can use this function to determine whether values in the a field are included in the b value set.

- Options

Parameter a: The input is a single-value multi-value INT32 /INT64 /string type<br />. Parameter b: The input is a constant string expression, which is separated by `|`.

- Return values

This function returns values of the BOOLEAN type. A value of true indicates that the value in the a field is included in the b value set. A value of false indicates that the value in the a field is excluded from the b value set.

- Examples

Retrieve all records with nid field value of [1,2,3] using `contain`

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

- Prototype
```
boolean notcontain(int32 a, const string b)
boolean notcontain(int64 a, const string b)
boolean notcontain(string a, const string b)
boolean notcontain(ARRAY<int32> a, const string b)
boolean notcontain(ARRAY<int64> a, const string b)
boolean notcontain(ARRAY<string> a, const string b)
```

- Descriptions

You can use this function to determine whether values in the a field are **not** included in the b value set.

- Options

Parameter a: The input is a single-value multi-value INT32 /INT64 /string type<br />. Parameter b: The input is a constant string expression, which is separated by `|`.

- Return values

This function returns values of the BOOLEAN type. A value of true indicates that the value in the a field is **not** included in the b value set. A value of false indicates that the value in the a field is included in the b value set.

- Examples

Retrieve all records with nid field value **not** in range [1,2,3] using `notcontain`

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

- Prototype
```
boolean MATCHINDEX(const string a, const string b)
boolean MATCHINDEX(const string a, const string b, const string c)
```

- Descriptions

Check whether field a **contains** the content described in field b. The recall <br />of a single-field index is only used for the inverted acceleration optimization in the index table recall phase. It is used in the WHERE condition.

- Options

Parameter a: The input is of the constant string type, which corresponds to the inverted optimization field<br />.
Parameter b: The input is of the constant string type, and the content is the string description content <br />
parameter c: optional. it can describe that the content contains word segmentors and stopwords, which are used to segment the content in b. Mainly through the following keyword settings, using`:``,` separate each item.

      - global_analyzer
      - specific_index_analyzer
      - no_token_indexes
      - tokenize_query
      - remove_stopwords
      - default_op
- Return values

This function returns values of the BOOLEAN type. A value of true indicates that the value in the a field includes the value of the b parameter. A value of false indicates that the value in the a field does not include the value of the b parameter.****

- Examples

In the following statement, the `MATCHINDEX` function is used to query data entries in the rows in which the `title` field values include the Lens keyword.

```sql
SELECT nid, brand FROM phone WHERE MATCHINDEX('title', 'lens')
```

```
------------------------------- TABLE INFO ---------------------------
                 nid |               brand |
                   1 |              Huawei |
```

<a name="JZXMG"></a>
#### QUERY

- Prototype
```
boolean QUERY(const string a, const string b)
boolean QUERY(const string a, const string b, const string c)
```

- Descriptions

Check whether field a **contains** the content described in field b, and provide automatic word segmentation and retrieval capabilities. <br />[ha3 query syntax](./ha3_query_clause) is used in SQL mode. <br />is only used in the index table recall phase to accelerate the optimization of inverted query. It is used in the WHERE condition.

- Options

Parameter a: The input is of the constant string type and will be used as the default index field<br />.
Parameter b: The input is of the constant string type, and the content is the string description content <br />
parameter c: optional, which can describe the content including word segmentors, stop words, etc., used to segment the content in b, use`:``,` separate each item, mainly set by keyword:

      - global_analyzer
      - specific_index_analyzer
      - no_token_indexes
      - tokenize_query
      - remove_stopwords
      - default_op
- Return values

This function returns values of the BOOLEAN type. A value of true indicates that the value in the a field includes the value of the b parameter. A value of false indicates that the value in the a field does not include the value of the b parameter.****

- Examples
   - Using the `QUERY`, query the entries that contain "Huawei mobile phone" in the `title`.
```sql
SELECT nid, price, brand, size FROM phone WHERE QUERY(title, 'Huawei mobile')
```

```
USE_TIME: 0.034, ROW_COUNT: 1

------------------------------- TABLE INFO ---------------------------
                 nid |               price |               brand |                size |
                   2 |                4388 |              Huawei |                 5.5 |
```


- Use a combination of conditions to search for entries that contain "Huawei mobile phone" or "OPPO mobile phone" in the `title`.

```sql
SELECT nid, price, brand, size FROM phone
   WHERE QUERY(title, 'Huawei mobile phone OR OPPO mobile phone')
```

```
USE_TIME: 0.03, ROW_COUNT: 2

------------------------------- TABLE INFO ---------------------------
                 nid |               price |               brand |                size |
                   2 |                4388 |              Huawei |                 5.5 |
                   4 |                2999 |                OPPO |                 5.5 |
```



- Specify the usage such as default_op. Separate the usage with`:` `and`
```sql
SELECT nid, price, brand, size FROM phone
   WHERE QUERY(title, 'Huawei mobile phone', 'default_op:OR,remove_stopwords:true')
```

- Zone ID

Parameter 2 of QUERY UDFs is parsed by the HA3 query parser. For more information about the HA3 query syntax, see[](./ha3_query_clause.md). When you specify a constant string in parameter 2, you must remove the single quotation marks before and after the HA3 query. For example, QUERY(title, 'Huawei mobile phone OPPO mobile') is equivalent to HA3 query=Huawei mobile phone OPPO mobile phone. If you use a logical operator in the query clause, you can enclose each constant in single quotation marks ('). For example, the query='Huawei Mobile Phone' AND 'OPPO Mobile Phone' clause in a statement is equivalent to the WHERE QUERY (('title', 'Huawei Mobile Phone' AND 'OPPO Mobile Phone') condition in a QUERY function.

- Common error
   | Error type | Error form | Correct form |
   | --- | --- | --- |
   | The syntax error is reported. No query result is found. | QUERY('pidvid','123:456') | QUERY('pidvid','"123:456"') |

<a name="VQz4F"></a>
####
<a name="UMfpo"></a>
#### hashcombine

- Prototype
```
int64 hashcombine(int64 k1, int64 k2, ...)
```

- Descriptions

You can use this function to merge values in multiple fields of the INT64 type into one value of the INT64 type. This function uses the built-in hash function to merge the specified values.

- Options

Multiple fields of the INT64 type.

- Return values

The value that is returned after values in the specified fields are merged based on the hash algorithm.

- Examples

In the following statement, the `hashcombine` function is used to merge values in the cate_id and user_id fields.
```sql
SELECT hashcombine(cate_id,user_id) FROM phone;
```
<a name="jZFoo"></a>
#### rangevalue

- Prototype
```
float rangevalue(float v, string desc)
```

- Descriptions

You can use this function to convert continuous values in the specified ranges to discrete values.

- Options

Parameter v: Column <br />of consecutive values Parameter desc: Mapping rule

- Return values

The values that are returned after the specified values are converted to the specified discrete values.

- Examples

In the following statement, the `rangevalue` function is used to convert values in the price field based on the following rules: Values that are smaller than or equal to 1000 are converted to 1.0, values that are greater than 1000 and smaller than 5000 are converted to 2.0, and other values are not converted.
```sql
SELECT rangevalue(price,'(,1000]:1.0;(1000,5000]:2.0') FROM phone;
```
<a name="ysbYg"></a>
#### sphere_distance

- Prototype
```
double sphere_distance(LOCATION point, double longitude, double latitude)
```

- Descriptions

You can use this function to calculate the distance between a document and the location that is determined by the specified latitude and longitude.

- Options

point: single-value column of the LOCATION type <br />longitude: longitude <br />latitude: latitude

- Return values

The spherical distance of the earth, in kilometers

- Examples

In the following statement, the `sphere_distance` function is used. The geo field must be a single-value field of the LOCATION type, and a forward index is created based on this field.
```sql
SELECT sphere_distance(geo,127.0,30.0) FROM phone
```

<a name="sCuT0"></a>
#### ha_in

- Prototype
```
boolean ha_in(int32 v, const string desc, [const string sep])
boolean ha_in(ARRAY<int32> v, const string desc, [const string sep])
boolean ha_in(int64 v, const string desc, [const string sep])
boolean ha_in(ARRAY<int64> v, const string desc, [const string sep])
boolean ha_in(string v, const string desc, [const string sep])
boolean ha_in(ARRAY<string> v, const string desc, [const string sep])
```

- Descriptions

You can use this function in a similar manner as the contain function. You can use this function to determine whether a value in a field is included in a specified value set. You can use this function to query data from a summary table, a key-value table, or a Pkey-Skey-value table. For more information, see [Query data from a summary table](./Summary query. md) and [Query data from a key-value table or a Pkey-Skey-value table](./KV, KVV query. md).

- Options

Parameter v: field, supports single-value multi-value type <br />parameter desc: constant description, can express multiple items, v meets one of the <br />parameter sep: optional, separator, split parameter desc content into multiple items, default `|`

- Return values

true: indicates that the value is included in the value of the desc parameter. false: indicates that the value is excluded from the value of the desc parameter.

- Examples

In the following example, the `ha_in` function is used to query data entries in the rows in which the nid field values are included in the value of the desc parameter.
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


## Appendix:

Parameters for MATCHINDEX and QUERY

* global_analyzer

   * Specifies a global analyzer. The analyzer that is specified by this parameter has a higher priority than the analyzer that you specified in the schema. The specified analyzer must be included in the analyzer.json file.

   - specific_index_analyzer: The specified index in the query uses another tokenizer. This tokenizer overwrites the tokenizer of the global_analyzer and schema.

   - no_token_indexes: The specified indexes in the query are not tokenised. The processes other than tokenisation, such as normalization and stopword removal, are executed normally. <br />``

   - tokenize_query: `true` or `false` indicates whether the query needs to be tokenized. `QUERY` the UDF has a tokenizer, this parameter does not take effect<br />. Default `true`

   - remove_stopwords: `true` or `false` indicates whether to delete stop words. The default `true` of stop words is configured <br />in the tokenizer.

   - default_op: specifies the join operator after the default query word segmentation used in the query, `AND` or `OR`. The default value is AND.
