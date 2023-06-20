# schema配置
一个完整的schema.json文件通常包括table_name、dictionaries、fields、indexs、attributes、summarys和source。
一个schema.json的例子：
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

以上各段说明如下：

<1> table名称，在cluster.json中的table_name就是指定这里的table_name；

<2> fields域，指定了数据的fields信息；

<3> indexs，倒排索引配置；

<4> attributes，指定做正排表的fields；

<5> summarys，指定summary的fields。

<6> source, 制定source的fields、modules。

<7> enable_ttl, 是否开启文档过期自动删除功能，如果开启的话，会自动添加doc_time_to_live_in_seconds到attribute中, 该字段为uint32类型。


# 其他
- schema支持的字段类型请参考[引擎内置字段类型](https://github.com/alibaba/havenask/wiki/%E5%BC%95%E6%93%8E%E5%86%85%E7%BD%AE%E5%AD%97%E6%AE%B5%E7%B1%BB%E5%9E%8B)
- 索引类型详情请参考[索引](https://github.com/alibaba/havenask/wiki/%E7%B4%A2%E5%BC%95)