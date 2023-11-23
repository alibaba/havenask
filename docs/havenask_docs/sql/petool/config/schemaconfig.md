---
toc: content
order: 2
---
# schema配置
## 结构

一个完整的schema.json文件通常包括columns和indexes
一个schema.json的例子：
```
{
    "columns": [                                        (1)
        {
            "name": "title",
            "type": "TEXT",
            "analyzer": "simple_analyzer"
        },
        {
            "name": "subject",
            "type": "TEXT",
            "analyzer": "simple_analyzer"
        },
        {
            "name": "id",
            "type": "UINT32"
        },
        {
            "name": "hits",
            "type": "UINT32"
        },
        {
            "name": "createtime",
            "type": "UINT64"
        }
    ],
    "indexes": [                                       (2)
        {
            "name": "id",
            "index_config" : {
                "index_fields": [
                    {
                        "field_name": "id"
                    }
                ]
            },
            "index_type": "PRIMARY_KEY64"               (3)
        },
        {
            "name": "id",
            "index_config" : {
                "index_fields": [
                    {
                        "field_name": "id"
                    }
                ]
            },
            "index_type": "ATTRIBUTE"                   (4)
        },
        {
            "name": "hits",
            "index_config" : {
                "index_fields": [
                    {
                        "field_name": "hits"
                    }
                ]
            },
            "index_type": "ATTRIBUTE"
        },
        {
            "name": "createtime",
            "index_config" : {
                "index_fields": [
                    {
                        "field_name": "createtime"
                    }
                ]
            },
            "index_type": "ATTRIBUTE"
        },
        {
            "name": "title",
            "index_config" : {
                "index_fields": [
                    {
                        "field_name": "title"
                    }
                ]
            },
            "index_type": "TEXT"
        },
        {
            "name": "subject",
            "index_config" : {
                "index_fields": [
                    {
                        "field_name": "subject"
                    }
                ]
            },
            "index_type": "TEXT"                        (5)
        },
        {
            "name": "default",
            "index_type": "PACK",
            "index_config": {
                "index_fields": [
                    {
                        "field_name": "title",
                        "boost": 100
                    },
                    {
                        "field_name": "subject",
                        "boost": 200
                    }
                ]
            }
        },
        {
            "name": "summary",
            "index_type": "SUMMARY",                     (6)
            "index_config": {
                "index_fields": [
                    {
                        "field_name": "title"
                    },

                    {
                        "field_name": "subject"
                    },
                    {
                        "field_name": "id"
                    },
                    {
                        "field_name": "hits"
                    },
                    {
                        "field_name": "createtime"
                    }
                ]
            }
        }
    ]
}
    

```

以上各段说明如下：

<1> columns，指定表的字段信息

<2> indexes，指定索引配置

<3> PRIMARY_KEY64，指定表的主键

<4> ATTRIBUTE, 指定attribute的fields

<5> TEXT，指定text类型的倒排索引的fields

<6> SUMMARY，指定summary的fields。



## 其他
- schema支持的字段类型请参考[引擎内置字段类型](../../indexes/field-type)
- 索引类型详情请参考[索引介绍](../../indexes/intro.md)