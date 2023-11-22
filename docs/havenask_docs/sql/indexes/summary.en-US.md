---
toc: content
order: 6
---



# summary index

A summary index can store information that corresponds to a document. OpenSearch Vector Search Edition can use the document ID to obtain the location in which the information is stored. This way, OpenSearch Vector Search Edition can provide you with the storage location of the information. The schema of a summary index is similar to the schema of a forward index. The features of the indexes are different.

### Configuration example
The name, index_type, and index_fields parameters are required. The other parameters are optional.
The type of the SUMMARY index. You can specify only one. You cannot specify multiple SUMMARY indexes.
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

- name: the name of the digest index.
- index_type: the type of the index, which must be SUMMARY.
- index_fields: specifies one or more fields (column).
- compress_type: specifies the file compression mode. You can configure one of ZSTD,SNAPPY,LZ4,LZ4HC, and ZLIB.
