<a name="Eoesh"></a>

Introduction to UDFs

<a name="GHATz"></a>

### Built-in user-defined functions (UDFs)

<a name="lvxHf"></a>



<a name="ad3GU"></a>

##### Complex type functions

| Function | Feature | Version |
| --- | --- | --- |
| [contain](#3mK2Q) | Determines that the field value is within a given set. A single value or multiple values are supported. | ALL |
| [notcontain](#k5miJ) | Determines that the field value is not within the given set. A single value or multiple values are supported. | ALL |
| [MATCHINDEX](#EoXPz) | Returns inverted indexes of the specified field based on the specified conditions. | ALL |
| [QUERY](#gQUxc) | Returns inverted indexes based on the specified conditions. The syntax of this function is the same as the syntax of the query clause supported by Havenask V3. | ALL |
| [sp_query_match](#pFQJA) | Returns the inverted indexes that match the conditions that you specified by using the sp syntax. | >=3.7.5 |
| [hashcombine](#VMScX) | Merges values in multiple fields of the INT64 type into one value of the INT64 type. | >=3.7.5 |
| [rangevalue](#vu5UD) | Converts values in the specified ranges in a field to values of the specified data type. | >=3.7.5 |
| [sphere_distance](sphere_distance) | Calculates a distance value based on the specified longitude and latitude. | >=3.7.5 |
| [ha_in](#NNN5S) | Determines that the field value is within a given set. | >=3.7.5 |
| [index](#pU3C1) | Returns the value for multi-value fields by given subscript. | ALL, required |





<a name="CAGWf"></a>

### Examples



#### Sample queries



- Query full data in the table



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

##### contain



- Syntax

```

boolean contain(int32 a, const string b)

boolean contain(int64 a, const string b)

boolean contain(string a, const string b)

boolean contain(ARRAY<int32> a, const string b)

boolean contain(ARRAY<int64> a, const string b)

boolean contain(ARRAY<string> a, const string b)

```



- Description



You can use this function to determine whether values in the a field are included in the b value set.



- Parameters



Parameter a: The input is of the int32/int64/string type <br />with single-value and multi-value parameters. Parameter b: The input is a constant string expression separated by `|`, indicating that any item is satisfied.



- Return values



This function returns values of the BOOLEAN type. true: indicates that the value in the a field is included in the b set. false: indicates that the value in the a field is excluded from the b value set.



- Examples



In the following statement, the `contain` function is used to query data entries in the rows in which the nid field value is 1, 2, or 3.[]



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

##### notcontain



- Syntax

```

boolean notcontain(int32 a, const string b)

boolean notcontain(int64 a, const string b)

boolean notcontain(string a, const string b)

boolean notcontain(ARRAY<int32> a, const string b)

boolean notcontain(ARRAY<int64> a, const string b)

boolean notcontain(ARRAY<string> a, const string b)

```



- Description



You can use this function to determine whether values in the a field are **not** included in the b value set.



- Parameters



a: specifies a field in the data table. The field that you specify can be a single-value field or multi-value field of the INT32, INT64, or STRING type.<br />b: specifies a constant string. The specified constants are separated with `|`. If a value matches one of the specified constants, this function returns false.



- Return values



This function returns values of the BOOLEAN type. true: indicates that the value in the a field is **not** included in the b value set. false: indicates that the value in the a field is included in the b value set.



- Examples



In the following statement, the `notcontain` is used to query data entries in the rows in which the nid field values are **not** 1, 2, or 3.[]



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

##### MATCHINDEX



- Syntax

```

boolean MATCHINDEX(const string a, const string b)

boolean MATCHINDEX(const string a, const string b, const string c)

```



- Description



You can use this function to determine whether a value in the a field **includes** the value of the b parameter. You can use this function to obtain the index of a field.<br />You can use this function in only the WHERE clause to optimize inverted indexing in the index retrieval phase.



- Parameters



a: specifies a field of the STRING type. The system creates an inverted index based on the specified field to optimize the query. <br />b: specifies a keyword of the STRING type. <br />The value of the b parameter can be used as a search query string for positional indexing. For more information, see [query clause](). <br />c: optional. You can specify an analyzer, stop words, and other information.<br />You can include the following parameters in the value of the c parameter to specify an analyzer, stop words, and other information:<br />You must specify a colon (`:`) between a parameter name and the corresponding value and separate multiple parameters with commas (`,`).



      - global_analyzer

      - specific_index_analyzer

      - no_token_indexes

      - tokenize_query

      - remove_stopwords

      - default_op

- Return values



This function returns values of the BOOLEAN type. true: indicates that the value in the a field includes the value of the b parameter. false: indicates that the value in the a field does not include the value of the b parameter.****



- Examples



In the following statement, the `MATCHINDEX` function is used to query data entries in the rows in which the `title` field values include the Lens keyword.



```sql

SELECT nid, brand FROM phone WHERE MATCHINDEX('title', 'Lens')

```



```

------------------------------- TABLE INFO ---------------------------

                 nid |               brand |

                   1 |              Huawei |

```



<a name="JZXMG"></a>

##### QUERY



- Syntax

```

boolean QUERY(const string a, const string b)

boolean QUERY(const string a, const string b, const string c)

```



- Description



You can use this function to determine whether a value in the a field **includes** the value of the b parameter. The system automatically converts the specified keyword to terms and performs a query.<br />This function supports the syntax of the query clause in SQL statements supported by Havenask V3. For information about the query clause, see [query clause]().<br />You can use this function in only the WHERE clause to optimize inverted indexing in the index retrieval phase.



- Parameters



a: specifies a field of the STRING type. The system automatically uses the specified field as the value of the default_index parameter in the config clause. For information about the config clause, see [config clause]().<br />b: specifies a keyword of the STRING type.<br />The system includes the value of the b parameter in the statement after the system optimizes the statement and queries data from the specified ranges. For more information, see [query clause]().<br />c: optional. You can specify an analyzer, stop words, and other information.<br />You must specify a colon (`:`) between a parameter name and the corresponding value and separate multiple parameters with commas (`,`).<br />You can include the following parameters in the value of the c parameter to specify an analyzer, stop words, and other information:



      - global_analyzer

      - specific_index_analyzer

      - no_token_indexes

      - tokenize_query

      - remove_stopwords

      - default_op

- Return values



This function returns values of the BOOLEAN type. true: indicates that the value in the a field includes the value of the b parameter. false: indicates that the value in the a field does not include the value of the b parameter.****



- Examples

   - In the following statement, the `QUERY` function is used to query data entries in the rows in which the `title` field values include the Huawei keyword.

```sql

SELECT nid, price, brand, size FROM phone WHERE QUERY(title, 'Huawei')

```



```

USE_TIME: 0.034, ROW_COUNT: 1



------------------------------- TABLE INFO ---------------------------

                 nid |               price |               brand |                size |

                   2 |                4388 |              Huawei |                 5.5 |

```





- In the following statement, the Huawei keyword and the OPPO keyword are used to query data entries in the rows in which the title field values include the Huawei keyword or the OPPO keyword.``



```sql

SELECT nid, price, brand, size FROM phone

   WHERE QUERY(title, 'Huawei OR OPPO')

```



```

USE_TIME: 0.03, ROW_COUNT: 2



------------------------------- TABLE INFO ---------------------------

                 nid |               price |               brand |                size |

                   2 |                4388 |              Huawei |                 5.5 |

                   4 |                2999 |                OPPO |                 5.5 |

```







- In the following statement, the default_op parameter is included in the value of the c parameter. You can include multiple parameters in the value of the c parameter by specifying a colon `:` between a parameter and the corresponding value and separating multiple parameters with commas`,`.

```sql

SELECT nid, price, brand, size FROM phone

   WHERE QUERY(title, 'Huawei OPPO Mobile Phone', 'default_op:OR,remove_stopwords:true')

```



- Remarks



Parameter 2 of the QUERY UDF is parsed by the query syntax parser of Havenask V3. For more information about the Havenask V3 query syntax, see [query clause](https://github.com/alibaba/havenask/wiki/query-clauses-en) . When you set parameter 2 to a constant string, remove the single quotation marks before and after when you use Havenask V3 for queries. For example, QUERY(title, 'Huawei OPPO ') is equivalent to query=Huawei OPPO by using Havenask V3. If you want to enclose quotation marks in the query description, for example, query='Huawei Mobile Phone' AND 'OPPO Mobile Phone' using Havenask V3, the equivalent form in QUERY UDF is QUERY(title, '''Huawei Mobile Phone" AND ''OPPO Mobile Phone"').



- Typical errors

   | Error type | Error form | Correct form |

   | --- | --- | --- |

   | Syntax error. No query result. | QUERY('pidvid','123:456') | QUERY('pidvid','"123:456"') |



<a name="VQz4F"></a>

####

<a name="UMfpo"></a>

##### sp_query_match



- Syntax

```

boolean sp_query_match(string a)

```



- Description



You can use this function to query inverted indexes based on the conditions that are specified by using the sp syntax.



- Parameters



a: specifies a search query in the sp format.



- Return values



- Examples



In the following statement, the `sp_query_match` function is used to query data entries in the rows in which the brand field value is Apple and the nid field value is 6, 7, or 8.

```sql

SELECT * FROM phone WHERE sp_query_match('brand:Apple+(nid:6|nid:7|nid:8'))

```

<a name="fcn81"></a>

##### hashcombine



- Syntax

```

int64 hashcombine(int64 k1, int64 k2, ...)

```



- Description



You can use this function to merge values in multiple fields of the INT64 type into one value of the INT64 type. This function uses the built-in hash function to merge the specified values.



- Parameters



Multiple fields of the INT64 type.



- Return values



The value that is returned after values in the specified fields are merged based on the hash algorithm.



- Examples



In the following statement, the `hashcombine` function is used to merge values in the cate_id and user_id fields.

```sql

SELECT hashcombine(cate_id,user_id) FROM phone;

```

<a name="jZFoo"></a>

##### rangevalue



- Syntax

```

float rangevalue(float v, string desc)

```



- Description



You can use this function to convert continuous values in the specified ranges to discrete values.



- Parameters



v: specifies the field in which the values that you want to convert are stored.<br />desc: describes the mapping rules.



- Return values



The values that are returned after the specified values are converted to the specified discrete values.



- Examples



In the following statement, the `rangevalue` function is used to convert values in the price field based on the following rules: Values that are not greater than 1000 are converted to 1.0. Values that are greater than 1000 but not greater than 5000 are converted to 2.0. Other values are not converted.

```sql

SELECT rangevalue(price,'(,1000]:1.0;(1000,5000]:2.0') FROM phone;

```

<a name="ysbYg"></a>

##### sphere_distance



- Syntax

```

double sphere_distance(LOCATION point, double longitude, double latitude)

```



- Description



You can use this function to calculate the distance between a document and the location specified by the latitude and longitude.



- Parameters



point: specifies a field of the LOCATION type. Values in the specified field must be in the single-value format.<br />longitude: specifies the longitude of a location.<br />latitude: specifies the latitude of a location.



- Return values



The values returned are the spherical distance, in kilometers.



- Examples



In the following statement, the `sphere_distance` function is used. The geo field must be a single-value field of the LOCATION type, and a forward index is created based on this field.

```sql

SELECT sphere_distance(geo,127.0,30.0) FROM phone

```



<a name="sCuT0"></a>

##### ha_in



- Syntax

```

boolean ha_in(int32 v, const string desc, [const string sep])

boolean ha_in(ARRAY<int32> v, const string desc, [const string sep])

boolean ha_in(int64 v, const string desc, [const string sep])

boolean ha_in(ARRAY<int64> v, const string desc, [const string sep])

boolean ha_in(string v, const string desc, [const string sep])

boolean ha_in(ARRAY<string> v, const string desc, [const string sep])

```



- Description



You can use this function in a similar manner as the contain function. You can use this function to determine whether a value in a field is included in a specified value set. You can use this function to query data from a summary table, a key-value table, or a Pkey-Skey-value table. For more information, see [Query data from a summary table](https://github.com/alibaba/havenask/wiki/Summary-Query-en) and [Query data from a key-value table or a Pkey-Skey-value table](https://github.com/alibaba/havenask/wiki/KV-Query-en).



- Parameters



v: specifies the field in which the data that you want to query is stored. The field can be a single-value field or multi-value field.<br />desc: specifies a value set. The value set can include multiple constants. If a value matches one of the specified constants, the function returns true.<br />sep: optional. Specifies the delimiter that is used to separate multiple constants in the value of the desc parameter. Default delimiter: `|`.



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



##### index



- Syntax

```

int8 index(ARRAY<int8> mv, const int64 idx, [const int8 def])

uint8 index(ARRAY<uint8> mv, const int64 idx, [const uint8 def])

int16 index(ARRAY<int16> mv, const int64 idx, [const int16 def])

uint16 index(ARRAY<uint16> mv, const int64 idx, [const uint16 def])

int32 index(ARRAY<int32> mv, const int64 idx, [const int32 def])

uint32 index(ARRAY<uint32> mv, const int64 idx, [const uint32 def])

int64 index(ARRAY<int64> mv, const int64 idx, [const int64 def])

uint64 index(ARRAY<uint64> mv, const int64 idx, [const uint64 def])

float index(ARRAY<float> mv, const int64 idx, [const float def])

string index(ARRAY<string> mv, const int64 idx, [const string def])

```



- Description



You can use this function to return the specific single value based on the given subscript from a multi-value field. You can specify the default value returned when the subscript is out of range.



- Parameters



mv: specifies the multi-value field of the NUMERIC or STRING type.<br />idx: specifies the specific subscript that is returned, a constant of the INT64 type.<br />def: optional. Specifies the value that is returned when the subscript overflows, a constant of the same type as the single-value field of the mv parameter. If this parameter is unspecified, the default value of the NUMERIC type is 0 and the default value of the STRING type is an empty string.



- Return values



The value of mv[idx] is returned when the subscript is valid and the value of def is returned when the subscript overflows.



- Examples

```sql

For the three documents with the prices field, the original content is as follows:

doc1: prices=1.0 2.0 3.0

doc2: prices=100.0

doc3: prices=1000.0 2000.0



If no default value is specified, the function is executed as follows:



select index(prices, 2) as price_2 from phone



USE_TIME: 32.141ms, ROW_COUNT: 2



------------------------------- TABLE INFO ---------------------------

                  price_2(double) |

                         2.0      |

                         0.0      |

                         2000.0   |





If the default value is specified, the function is executed as follows:



select index(prices, 2， -1.0) as price_2 from phone



USE_TIME: 32.141ms, ROW_COUNT: 2



------------------------------- TABLE INFO ---------------------------

                  price_2(double) |

                         2.0      |

                        -1.0      |

                         2000.0   |

```

<a name="LIC3Z"></a>

##### array_slice



- Syntax

```

ARRAY<int8> index(ARRAY<int8> mv, const int64 begin, const int64 length)

ARRAY<uint8> index(ARRAY<uint8> mv, const int64 begin, const int64 length)

ARRAY<int16> index(ARRAY<int16> mv, const int64 begin, const int64 length)

ARRAY<uint16> index(ARRAY<uint16> mv, const int64 begin, const int64 length)

ARRAY<int32> index(ARRAY<int32> mv, const int64 begin, const int64 length)

ARRAY<uint32> index(ARRAY<uint32> mv, const int64 begin, const int64 length)

ARRAY<int64> index(ARRAY<int64> mv, const int64 begin, const int64 length)

ARRAY<uint64> index(ARRAY<uint64> mv, const int64 begin, const int64 length)

ARRAY<float> index(ARRAY<float> mv, const int64 begin, const int64 length)

ARRAY<double> index(ARRAY<double> mv, const int64 begin, const int64 length)

ARRAY<string> index(ARRAY<string> mv, const int64 begin, const int64 length)

```



- Description



You can use this function to return the subset of a given subscript from a multi-value field by its beginpoint and number.



- Parameters



mv: specifies the multi-value field of the NUMERIC or STRING type.<br />begin: specifies the beginpoint of the subscript that is returned. The value is a constant of the INT64 type.<br />length: specifies the number of values from the beginpoint. The value is a constant of the INT64 type.



- Return values



The value of mv within the range from begin to begin + length-1 is returned. The value is truncated based on the value of mv.



- Examples

```sql

For the three documents with the prices field, the original content is as follows:

doc1: prices=1.0 2.0 3.0

doc2: prices=100.0

doc3: prices=1000.0 2000.0





select array_slice(prices, 1, 2) as price_2 from phone



USE_TIME: 32.141ms, ROW_COUNT: 2



------------------------------- TABLE INFO ---------------------------

            price_2(multi_double) |

                     2.0 3.0      |

                                  |

                     2000.0       |

```



<a name="uu7k2"></a>

### Appendix:

<a name="U3sqX"></a>

###

<a name="ORuoy"></a>

#### Analyzer settings

You can configure the following parameters to specify analyzers when you use functions such as MATCHINDEX and QUERY.



- [global_analyzer]()



Specifies a global analyzer. The analyzer that is specified by this parameter overrides the analyzer that you specified in the schema. The specified analyzer must be included in analyzer.json.



- [specific_index_analyzer]()



Specifies an analyzer for the specified field. The analyzer that is specified by this parameter overrides the analyzer that you specified in the global_analyzer parameter and the analyzer that you specified in the schema.



- [no_token_indexes]()



The analysis process does not need to be performed for the index specified in the query. Other processes such as normalization and stop word removal can still be performed.``<br />



- tokenize_query



`true`: specifies to convert the search query to terms. `false`: specifies to not convert the search query to terms. If you configured an analyzer when you specified the `QUERY` UDF, this parameter does not take effect.<br />The default value is `true`.



- remove_stopwords



`true`: specifies to delete stop words. `false`: specifies to not delete stop words. You can specify stop words when you configure the analyzer.<br />The default value is `true`.



- [default_op]()



Specifies the logical relationship between the terms that are returned after the system tokenizes the search query by using the default analyzer. Valid values: `AND` and `OR`. You can specify a default operator when you configure biz. For information about how to configure biz, see [biz configuration]().





<a name="P1RjJ"></a>

##

<a name="HbLE3"></a>

### Supported UDFs from third-parties

<a name="sFpn7"></a>

##### Complex type functions

| Function | Feature | Version |
| --- | --- | --- |
| type cast | Converts values from one data type to another data type. | ALL |
|  |  |  |
