---
toc: content
order: 6
---



# 摘要索引

摘要索引将一片文档对应的信息存储在一起，通过docID可以定位该文档信息的存储位置，从而为用户提供摘要信息的存储获取服务。摘要索引的结构和正排索引类似,只是完成的功能不同 。

### 配置示例
name、index_type、index_fields为必须配置的项，其余为可选配置。
SUMMARY索引类型，只能配置1个，不能配多个SUMMARY索引。
```json
{
    "indexes": [
        {
            "name": "summary",
            "index_type": "SUMMARY",
            "index_config": {
                "index_fields": [
                    {
                        "field_name": "int8"
                    },
                    {
                        "field_name": "float"
                    },
                    {
                        "field_name": "string"
                    }
                ],
                "compress_type": "ZLIB"
            }
        }
    ]
}
```

- name：摘要索引名字
- index_type：索引的类型，必须为SUMMARY。
- index_fields：可以指定一个或多个field（column）
- compress_type：指定文件压缩方式，可配置ZSTD,SNAPPY,LZ4,LZ4HC,ZLIB中的一种
