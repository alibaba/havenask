---
toc: content
order: 5
---

# forward index

A forward index is an index that stores the values of a specific field (attribute) in a document and is used for filtering, counting, sorting, or scoring. The forward index is also known as an attribute index or a profile index. The word "forward" in the forward index refers to the process of converting documents to field information in the documents.
## Attribute types of forward indexes

The engine supports the following attribute types.

| Attribute type | Description | Single-value | Multivalue |
| --- | --- | --- | --- |
| INT8 | Stores 8-bit signed integers. | id=-27 | id=-27]26]33 |
| UINT8 | Stores 8-bit unsigned integers. | id=56 | id=27]222]32 |
| INT16 | Stores 16-bit signed integers. | id=-2724 | id=-1217^]236 |
| UINT16 | Stores 16-bit unsigned integers. | id=65531 | id=-65531]22236]0^]1 |
| INT32 | Stores 32-bit signed integers. | id=-655312 | id=-2714442]2344126]33441 |
| UINT32 | Stores 32-bit unsigned integers. | id=65537 | id=4011341512]26]33 |
| INT64 | Stores 64-bit signed integers. | id=-551533527 | id=-2416664447]236]133 |
| UINT64 | Stores 64-bit unsigned integers. | id=23545114533527 | id=12416664447]121436]2 |
| FLOAT | Stores single-precision 32-bit floating-point numbers. | id=3.14 | id=3.25]3.50]3.75 |
| DOUBLE | Stores double-precision 64-bit floating-point numbers. | id=3.1415926 | id=-3.1415926]26.1444]55.1441 |
| STRING | Stores strings. | id=abc | id=abc]def]dgg^]dd |


## Configuration example
The name, index_type, and index_fields parameters are required. Other parameters are optional.
You can specify only one field(column) for index_fields.
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

- name: the name of the forward index, which must be consistent with index_fields.
- index_type: the type of the index, which must be ATTRIBUTE.
- index_fields: You can specify only one field(column), which must be the same as the name. Fields of the text type are not supported.
- compress_type: specifies the file compression mode. You can configure one of ZSTD,SNAPPY,LZ4,LZ4HC, and ZLIB.
