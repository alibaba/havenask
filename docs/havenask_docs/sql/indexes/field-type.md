---
toc: content
order: 1 
---

# 字段类型介绍
| **字段类型** | **字段表述** | **是否支持多值** | **是否可以用于正排索引** | **是否可以用于摘要索引** | **是否可以用于倒排索引** |
| --- | --- | --- | --- | --- | --- |
| TEXT | 文本类型 | 否 | 否 | 是 | 是 |
| STRING | 字符串类型 | 是 | 是 | 是 | 是 |
| INT8 | 8位有符号数字类型 | 是 | 是 | 是 | 是 |
| UINT8 | 8位无符号数字类型 | 是 | 是 | 是 | 是 |
| INT16 | 16位有符号数字类型 | 是 | 是 | 是 | 是 |
| UINT16 | 16位无符号数字类型 | 是 | 是 | 是 | 是 |
| INT32 | 32位有符号数字类型 | 是 | 是 | 是 | 是 |
| UINT32 | 32位无符号数字类型 | 是 | 是 | 是 | 是 |
| INT64 | 64位有符号数字类型 | 是 | 是 | 是 | 是 |
| UINT64 | 64位无符号数字类型 | 是 | 是 | 是 | 是 |
| FLOAT | float类型（32位）的浮点 | 是 | 是 | 是 | 否 |
| DOUBLE | double类型（64位）的浮点 | 是 | 是 | 是 | 否 |
| RAW | 原始数据类型，用于向量检索 | 否 | 否 | 否 | 是 |

- TEXT字段类型在schema配置时需要指定分析器，如果不指定，默认为simple_analyzer。
### 配置方式
示例：
```json
{
    "columns": [
        {
            "name": "int8",
            "type": "INT8",
            "primary_key": true
        },
        {
            "name": "int8s",
            "type": "INT8",
            "multi_value": true,
            "separator": "#",
            "default_value": "100",
            "nullable": true,
            "updatable": true
        },
        {
            "name": "uint32",
            "type": "UINT32"
        },
        {
            "name": "int32",
            "type": "INT32",
            "default_value": "100"
        },
        {
            "name": "int64",
            "type": "INT64"
        },
        {
            "name": "float",
            "type": "FLOAT"
        },
        {
            "name": "double",
            "type": "DOUBLE",
            "updatable": true
        },
        {
            "name": "string",
            "type": "STRING",
            "nullable": true
        },
        {
            "name": "strings",
            "type": "STRING"
        },
        {
            "name": "text",
            "type": "TEXT",
            "analyzer": "simple_analyzer"
        },
        {
            "name": "texts",
            "type": "TEXT",
            "analyzer": "simple_analyzer"
        },
        {
            "name": "raw",
            "type": "RAW"
        }
    ]
}
```
 说明：
name和type为必填项，另外必须指定某一个column为primary key，即指定"primary_key": true。其余配置项为可选配置。

- **name**：字段名称。
- **type**：字段基本类型，其取值可为TEXT，STRING，INT8，UINT8，INT16，UINT16，INT32， UINT32，INT64，UINT64，FLOAT，DOUBLE，RAW中的一种。其中STRING不分词，TEXT分词。RAW是原始数据字段，自解释，用于向量索引。
- **primary_key**：是否为primary key字段，即是否为唯一值字段，默认值为false。只允许一个字段配置成primary key，且必须有一个字段配置成true。
- **multi_value**：表明该field为多值（如果用于建立attirubte则建立多值attribute），默认值为false。
- **separator**：对于多值字段，指定多值之间的分隔符。默认值为'^]'，ascii码为0x1D。
- **analyzer**：类型为TEXT的字段所用的分词器。 field_type为TEXT的field如果不配置，则默认使用simple analyzer。其他类型不允许配置。analyzer。
- **nullable**：指定该字段是否允许空值。默认值为false，如果配置了enable_null，则倒排可以用“__NULL__”召回空值的doc。正排可以查询到这个字段是否为空值。如果不配置该项，对于正排，数值类型为0，string和text类型为空。
- **default_value**：指定默认值，对于文档中不存在的column，可以指定默认值。
- **updatable**：表示该字段的正排是否可以更新。支持字段类型为：INT8，UINT8，INT16，UINT16，INT32，UINT32，INT64，UINT64，FLOAT，DOUBLE，STRING。不配置时，对于单值非string字段，默认为true，对于多值字段，默认为false。

