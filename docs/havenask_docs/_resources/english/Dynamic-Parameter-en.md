### Overview

Dynamic parameters are similar to [PrepareStatement](https://en.wikipedia.org/wiki/Prepared_statement) in databases. You can use question marks (?) as placeholders in a SQL statement, and use dynamic parameters to specify the actual values of the placeholders. The Iquan component of Havenask automatically converts the placeholders to the values of dynamic parameters.



Dynamic parameters can help improve the hit ratio of execution plans that the system caches. If you write SQL statements based on the same template and use dynamic parameters in the SQL statement, query performance is improved.



Note: You can use dynamic parameters to replace only values. You cannot use dynamic parameters to replace keywords or fields.



### Parameter description



| Paramater | Description | Example |

|-- | -- | -- |

iquan.plan.prepare.level | Specifies the type of the optimization result of a specific phase. When dynamic parameters are used, placeholders in the result are replaced with the parameter values that you specify.  The default value is jni.post.optimize.  |kvpair=...;iquan.plan.prepare.level:jni.post.optimize;... |

dynamic_params | Specifies the parameter values with which you want to replace placeholders in the SQL statements.  Note: When you want to use dynamic parameters to replace a field whose value is a placeholder in the SQL statements, make sure that the data type of the dynamic parameters must be the same as that of the field. Note: The values that you specify for dynamic parameters are two-dimensional arrays. Each one-dimensional array in the specified value of a dynamic parameter corresponds to a SQL statement.  | kvpair=...;dynamic_params=[[1, 1.23, "str"]] |

iquan.plan.cache.enable | Specifies whether to enable the feature of caching the execution plans based on the iquan.plan.prepare.level parameter. If you use a dynamic parameter for an SQL statement and enable the execution plan cache feature, Havenask caches the execution plan of the SQL statement. When you use the same SQL statement again, Havenask obtains the execution plan from the cache. In this case, the query execution time is reduced.  | kvpair=...;iquan.plan.cache.enable:true;... |



### Example

1. In the following sample statement, only dynamic parameters are used.

```

SELECT i1, cast(? as bigint) FROM t1 WHERE (i2 > 5 AND d3 < 10.1) OR s5 = ?

```

Before you use placeholders (?) in an SQL statement, you must specify dynamic parameters by using the kvpair clause. The following sample code shows how to specify dynamic parameters in the kvpair clause:



```

kvpair=...;

			 iquan.plan.prepare.level:jni.post.optimize;

       dynamic_params:[[10, "str5"]];

       ...;

```

2. In the following sample statement, dynamic parameters are used and the execution plan cache feature is enabled.



```

SELECT

    price,

    title,

    compute(

        longitude,

        latitude,

        city_id,

        CAST(? AS double),

        CAST(? AS double),

        CAST(1 AS bigint)

    ) AS distance

FROM

    store,

    unnest(store.sub_table)

WHERE

    MATCHINDEX('shop', ?)

    AND QUERY(name, ?)

```



The following sample code shows how to specify dynamic parameters in the kvpair clause:

```

kvpair= ...;

			 iquan.plan.cache.enable:true;

			 iquan.plan.prepare.level:jni.post.optimize;

       dynamic_params:[[119.98844256998,

                        36.776817017143,

                        "excellect",

                        "Fruit OR Watermelon"]]

       ...

```
