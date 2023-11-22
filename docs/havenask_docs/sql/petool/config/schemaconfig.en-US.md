---
toc: content
order: 2
---
# schema Configuration
## Structure

A complete schema.json file typically includes table_name, dictionaries, fields, indexs, attributes, summarys, and source.
An example of schema.json:
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
    ].
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
    ].
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

The above paragraphs are explained as follows:

&lt; 1&gt;  table name, table_name in cluster.json is the table_name specified here;

&lt; 2&gt;  fields Indicates the fields of data.

&lt; 3&gt;  indexs, inverted index configuration;

&lt; 4&gt;  attributes, which specifies the fields to be used for the positive list;

&lt; 5&gt;  summarys, which specifies the summary fields.

&lt; 6&gt;  source, specify the fields and modules of the source.

&lt; 7&gt;  enable_ttl: Specifies whether to enable the function of automatically deleting documents after expiration. If yes, doc_time_to_live_in_seconds is automatically added to the attribute list. The field is of type uint32.


## Other
- schema For the supported field types, see [Engine Built-in Field Type](.. /.. /indexes/field-type)
- Index type For details, see [Index Introduction](.. /.. /indexes/intro.md)