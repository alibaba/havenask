## Overview

You can use logical tables to construct multiple rows of data. In most cases, logical tables are used together with built-in table-valued functions (TVFs). You can also use the VALUES keyword to construct data. Compared with the VALUES keyword, logical tables provide the following advantages:

1. Each logical table contains a schema.

2. The execution plan can be cached.

## Examples

### Register a logical table

Register the logical table in the biz/sql/{bizname}_logictable.json directory of query result searcher (QRS) and specify the schema of the logical table. The following sample code provides an example on how to register two logical tables.



```

{

    "tables": [

        {

            "catalog_name": "default",

            "database_name": "item",

            "table_version": 2,

            "table_name": "trigger_table",

            "table_type": "json_default",

            "table_content_version": "0.1",

            "table_content": {

                "table_name": "trigger_table",

                "table_type": "logical",

                "fields": [

                    {

                        "field_name": "trigger_id",

                        "field_type": {

                            "type": "int64"

                        }

                    },

                    {

                        "field_name": "ratio",

                        "field_type": {

                            "type": "float"

                        }

                    }

                ],

                "sub_tables": [

                ],

                "distribution": {

                    "partition_cnt": 1,

                    "hash_fields": [

                        "trigger_id"

                    ],

                    "hash_function": "HASH64"

                },

                "join_info": {

                    "table_name": "",

                    "join_field": ""

                },

                "properties": {}

            }

        },

        {

            "catalog_name": "default",

            "database_name": "item",

            "table_version": 2,

            "table_name": "simple_table",

            "table_type": "json_default",

            "table_content_version": "0.1",

            "table_content": {

                "table_name": "simple_table",

                "table_type": "logical",

                "fields": [

                    {

                        "field_name": "int64_id",

                        "field_type": {

                            "type": "int64"

                        }

                    },

                    {

                        "field_name": "string_id",

                        "field_type": {

                            "type": "string"

                        }

                    }

                ],

                "sub_tables": [

                ],

                "distribution": {

                    "partition_cnt": 1,

                    "hash_fields": [

                        "int64_id"

                    ],

                    "hash_function": "HASH64"

                },

                "join_info": {

                    "table_name": "",

                    "join_field": ""

                },

                "properties": {}

            }

        }

    ]

}



```



### Use a logical table to construct data

<strong>Example 1 </strong>



```

SELECT *

FROM table (

  inputTableTvf(

    '100,0.1;200,0.2',

    (SELECT trigger_id, ratio FROM trigger_table)

  )

)

```

| trigger_id(float) | ratio(float) |

|------|------|

| 100 | 0.1 |

| 200 | 0.2 |



<strong>Example 2</strong>



```

SELECT *

FROM table (

  inputTableTvf(

    '?',

    (SELECT trigger_id, ratio FROM trigger_table)

  )

)

```

Add the following information to the kvpair clause: iquan.plan.prepare.level:rel.post.optimize;urlencode_data:true;dynamic_params:%5b%5b%27100%3a0.1%3b200%3a0.2%27%5d%5d



The value of the dynamic_params parameter is ['100:0.1;200:0.2']. The value is encoded by the urlencode function.





<strong>Example 3</strong>



```

SELECT *

FROM table (

  inputTableTvf(

    'aaa;bbb;ccc;ddd',

    (SELECT string_id FROM simple_table)

  )

)

```

| string_id(multi_char) |

|------|

| aaa |

| bbb |

| ccc |

| ddd |
