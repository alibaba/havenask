## Configure a schema

A complete schema.json file usually contains table_name, dictionaries, fields, indexes, attributes, summarys, and source.

Sample schema.json file:

```

{

    "table_name": "sample",                                    (1)

    "fields":                                                  (2)

    [

        {"field_name":"title",        "field_type":"TEXT",    "analyzer":"taobao_analyzer"},

        {"field_name":"user_name",    "field_type":"STRING" },

        {"field_name":"user_id",      "field_type":"INTEGER"},

        {"field_name":"price",        "field_type":"INTEGER" },

        {"field_name":"category",     "field_type":"INTEGER",  "multi_value":true },

        {"field_name":"product_id",   "field_type":"LONG" },

        {"field_name":"body",         "field_type":"TEXT",    "analyzer":"taobao_analyzer"},

        {"field_name":"b2b_body",     "field_type":"TEXT",    "analyzer":"b2b_analyzer"},

        {"field_name":"taobao_body1",  "field_type":"TEXT",    "analyzer":"taobao_analyzer"},

        {"field_name":"taobao_body2",  "field_type":"TEXT",    "analyzer":"taobao_analyzer"}

    ],

    "indexs":                                                  (3)

    [

        {

            "index_name": "pack_index",

            "index_type" : "PACK",

            "term_payload_flag" : 1,

            "doc_payload_flag" : 1,

            "position_payload_flag" : 1,

            "position_list_flag" : 1,

            "high_frequency_dictionary" : "top10",

            "high_frequency_term_posting_type" : "both",

            "index_fields":

            [

                {"field_name":"title", "boost":1000},

                {"field_name":"body", "boost":10}

            ]

        },

        {

            "index_name": "text_index",

            "index_type": "TEXT",

            "term_payload_flag" :  1 ,

            "doc_payload_flag" :  1 ,

            "position_payload_flag" : 1,

            "position_list_flag" : 1,

            "index_fields": "title"

        },

        {

            "index_name": "string_index",

            "index_type": "STRING",

            "index_fields": "user_name",

            "term_payload_flag" :  1 ,

            "doc_payload_flag" :  1

        },

        {

            "index_name": "primary_key_index",

            "index_type" : "PRIMARYKEY64",

            "index_fields": "product_id",

            "has_primary_key_attribute": true

        },

    ],

     "attributes": [ "id", "company_id", "cat_id" ],                               (4)

     "summarys":{                                                                  (5)

         "summary_fields": ["id", "company_id", "subject", "cat_id"],

         "compress":false

     },

     "src":{                                                                       (6)

                 "group_configs": {"field_mode":"all_field"}

     },

     "enable_ttl" : true,                                                          (7)

}

```



Field description:



(1) table_name: specifies the name of the table, which is the table_name  parameter in cluster.json.



(2) fields: specifies the fields information of the data.



(3) indexs: specifies the configurations of inverted indexes.



(4) attributes: specifies the fields on which attribute indexes are created.



(5) summarys: specifies the fields on which summary indexes are created.



(6) source: specifies the fields and modules of the source.



(7) enable_ttl: specifies whether to enable the automatic deletion of expired documents. If this feature is enabled, the doc_time_to_live_in_seconds parameter is automatically added to the document attributes. The value of the field is of the UINT32 type.





## Others

- For more information about the data types of the fields supported by the schema, see [Built-in field types of Havenask](https://github.com/alibaba/havenask/wiki/Data-Types-en).

- For more information about index types, see [Indexes](https://github.com/alibaba/havenask/wiki/Indexes-en).