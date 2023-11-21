---
toc: content
order: 5
---

# 正排索引

正排索引，也叫attribute索引或者profile索引，是存储doc某特定字段（正排字段）对应值的索引，用来进行过滤，统计，排序或者算分使用。正排索引中“正"指的是从doc-->doc fieldInfo的过程 。
## 正排索引的类型

目前引擎支持的正排字段的基本类型为: 

| attribute类型 | 描述 | 单值 | 多值 |
| --- | --- | --- | --- |
| INT8 | 8位有符号数字类型 | id=-27 | id=-27]26]33 |
| UINT8 | 8位无符号数字类型 | id=56 | id=27]222]32 |
| INT16 | 16位有符号数字类型 | id=-2724 | id=-1217^]236 |
| UINT16 | 16位无符号数字类型 | id=65531 | id=-65531]22236]0^]1 |
| INT32 | 32位有符号数字类型 | id=-655312 | id=-2714442]2344126]33441 |
| UINT32 | 32位无符号数字类型 | id=65537 | id=4011341512]26]33 |
| INT64 | 64位有符号数字类型 | id=-551533527 | id=-2416664447]236]133 |
| UINT64 | 64位无符号数字类型 | id=23545114533527 | id=12416664447]121436]2 |
| FLOAT | float类型（32位）的浮点 | id=3.14 | id=3.25]3.50]3.75 |
| DOUBLE | double类型（64位）的浮点 | id=3.1415926 | id=-3.1415926]26.1444]55.1441 |
| STRING | 字符串类型 | id=abc | id=abc]def]dgg^]dd |


## 配置示例
name、index_type、index_fields为必选配置，其余可选。
index_fields只能配置一个field(column)。
```json
{
    "indexes": [
        {
            "name": "int8s",
            "index_type": "ATTRIBUTE",
            "index_config": {
                "index_fields": [
                    {
                        "field_name": "int8s"
                    }
                ],
                "compress_type": "ZLIB"
            }
        },
        {
            "name": "uint32",
            "index_type": "ATTRIBUTE",
            "index_config": {
                "index_fields": [
                    {
                        "field_name": "uint32"
                    }
                ],
                "compress_type": "ZLIB"
            }
        },
        {
            "name": "double",
            "index_type": "ATTRIBUTE",
            "index_config": {
                "index_fields": [
                    {
                        "field_name": "double"
                    }
                ],
                "compress_type": "ZLIB"
            }
        },
        {
            "name": "strings",
            "index_type": "ATTRIBUTE",
            "index_config": {
                "index_fields": [
                    {
                        "field_name": "strings"
                    }
                ],
                "compress_type": "ZLIB"
            }
        }
    ]
}
```

- name：正排索引名字，必须和index_fields保持一致
- index_type：索引的类型，必须为ATTRIBUTE。
- index_fields：只能指定一个field（column），必须和name保持一致，不支持text类型的字段
- compress_type：指定文件压缩方式，可配置ZSTD,SNAPPY,LZ4,LZ4HC,ZLIB中的一种
