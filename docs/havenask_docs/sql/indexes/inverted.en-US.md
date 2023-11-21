---
toc: content
order: 4
---

# inverted index

<div class="lake-content" typography="classic"><h2 id="gD4Vi" style="font-size: 24px; line-height: 32px; margin: 21px 0 5px 0"><span class="ne-text">What is an inverted index?</span></h2><p id="u291aeb99" class="ne-p" style="margin: 0; padding: 0; min-height: 24px"><span class="ne-text"><br /></span><span class="ne-text">inverted index, also commonly referred to as inverted index, placed file, or inverted file, is an indexing method that is used to store the mapping of the storage location of a word in a document or group of documents under full-text search. An inverted index is the most commonly used data structure in document retrieval systems.  <br /></span><span class="ne-text">You can use inverted indexes to quickly locate the list of documents where a word is located, the position of the word in the document, and the word frequency. for analysis.  </span></p><h2 id="IQhVR" style="font-size: 24px; line-height: 32px; margin: 21px 0 5px 0"><span class="ne-text">Storage information of inverted indexes</span></h2>

Message Name | Description
-- | --
Ttf | Full name: total term frequency, indicating the total number of times the search term appears in all documents.
df | Full name: document frequency, which indicates the total number of documents that contain the search term.
tf | Full name: term frequency, which indicates the number of times the search term appears in the document.
docid | Full name: document id, which is the unique identifier of a document in the engine. You can use the docid to obtain other information about the original document.
fieldmap | Full name: field map, which is used to record field information that contains search terms.
Section Information | Users can segment certain documents and then add additional information to each segment. The information can be retrieved for subsequent processing.
position | Used to record the position information of the search term in the document
positionpayload | full name: position payload. users can set payload information for different positions of the document and retrieve it during retrieval for subsequent processing.
Docpayload | Full name: document payload. Users can add subsidiary information to some documents and take it out during retrieval for subsequent processing.
termpayload | Full name: term payload. Users can add subsidiary information for certain words and retrieve it during retrieval for subsequent processing.

<p id="uebc11eff" class="ne-p" style="margin: 0; padding: 0; min-height: 24px"><br></p><h2 id="ROOg1" style="font-size: 24px; line-height: 32px; margin: 21px 0 5px 0"><span class="ne-text">Basic process of inverted index retrieval</span></h2><p id="uefab12c0" class="ne-p" style="margin: 0; padding: 0; min-height: 24px"><span class="ne-text"><br /></span><span class="ne-text">When a user queries the inverted index of the word M, the engine first queries the dictionary file to find the starting position of the index word in the posting index file. <br /></span><span class="ne-text">Then, the engine parses the inverted row chain to obtain the three parts of information stored in the inverted row chain: TermMeta,DocList, and PositionList. <br /></span><span class="ne-text">TermMeta stores the basic description of index words, including the df, ttf, and termpayload information of the words. <br /></span><span class="ne-text">DocList contains the document information of the index word. The document information includes: DocumentId, search word frequency (tf), docpayload, and field information (termfield). <br /></span><span class="ne-text">PositionList is a list of the position information of search terms in a document, including the specific position and positionpayload information of search terms in the document.  </span></p><p id="u959f4b30" class="ne-p" style="margin: 0; padding: 0; min-height: 24px"><br></p><p id="u40689884" class="ne-p" style="margin: 0; padding: 0; min-height: 24px"><img src="https://cdn.nlark.com/lark/0/2018/png/114751/1541056605570-e4f5b3f9-0b89-4fc5-9949-b31c2355621f.png" width="546" id="flyif" class="ne-image"></p></div>


### Configuration example
The name, index_type, and index_fields parameters are required. The other parameters are optional.
For a PACK index, you can specify one or more fields in index_fields. For other types of inverted indexes, you can specify only one field in index_fields.
```json
{
    "indexes": [
        {
            "name": "uint32",
            "index_type": "NUMBER",
            "index_config": {
                "index_fields": [
                    {
                        "field_name": "uint32"
                    }
                ],
                "compress_type": "SNAPPY"
            }
        },
        {
            "name": "int8s",
            "index_type": "NUMBER",
            "index_config": {
                "index_fields": [
                    {
                        "field_name": "int8s"
                    }
                ],
                "compress_type": "ZSTD"
            }
        },
        {
            "name": "string",
            "index_type": "STRING",
            "index_config": {
                "index_fields": [
                    {
                        "field_name": "string"
                    }
                ],
                "compress_type": "LZ4"
            }
        },
        {
            "name": "strings",
            "index_type": "STRING",
            "index_config": {
                "index_fields": [
                    {
                        "field_name": "strings"
                    }
                ],
                "compress_type": "LZ4HC"
            }
        },
        {
            "name": "text",
            "index_type": "TEXT",
            "index_config": {
                "index_fields": [
                    {
                        "field_name": "text"
                    }
                ],
                "compress_type": "ZLIB"
            }
        },
        {
            "name": "texts",
            "index_type": "TEXT",
            "index_config": {
                "index_fields": [
                    {
                        "field_name": "texts"
                    }
                ],
                "compress_type": "ZLIB"
            }
        },
        {
            "name": "range",
            "index_type": "RANGE",
            "index_config": {
                "index_fields": [
                    {
                        "field_name": "int32"
                    }
                ],
                "compress_type": "ZLIB"
            }
        },
        {
            "name": "pack",
            "index_type": "PACK",
            "index_config": {
                "index_fields": [
                    {
                        "field_name": "text",
                        "boost": 100
                    },
                    {
                        "field_name": "texts",
                        "boost": 200
                    }
                ],
                "compress_type": "ZLIB"
            }
        },
        {
            "name": "int8",
            "index_type": "PRIMARY_KEY64",
            "index_config": {
                "index_fields": [
                    {
                        "field_name": "int8"
                    }
                ]
            }
        }
    ]
}
```
### PACK indexes
#### Introduction to PACK indexes
A PACK index is a multi-field index that is created on fields of the TEXT type. Compared with a TEXT index, the PACK index is created by merging multiple fields of the TEXT type for retrieval. The PACK index can also store section information so that the section where each search term is located and the related information can be queried.  You can use truncation and high-frequency words bitmap and tfbitmap to improve retrieval performance.

| **Parameter** | **df** | **ttf** | **tf** | **fieldmap** | **position** | **positionpayload** | **docpayload** | **termpayload** |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Supported or not | Supported. | Optional | Optional | Not Supported. | Optional | Optional | Optional | Optional |

#### Example for configuring a pack index
```
{
    "name": "pack_index",
    "index_type": "PACK",
    "index_config": {
        "index_fields": [
            {
                "field_name": "subject",
                "boost": 200000
            },
            {
                "field_name": "company_name",
                "boost": 600000
            },
            {
                "field_name": "feature_value",
                "boost": 600000
            },
            {
                "field_name": "summary",
                "boost": 600000
            }
        ],
        "index_params": {
            "term_payload_flag": "1",
            "doc_payload_flag": "1",
            "position_list_flag": "1",
            "position_payload_flag": "1",
            "term_frequency_flag": "1",
            "term_frequency_bitmap": "1",
            "high_frequency_dictionary": "bitmap1",
            "high_frequency_adaptive_dictionary": "df",
            "high_frequency_term_posting_type": "both",
            "index_analyzer": "simple_analyzer",
            "format_version_id": "1"
        },
        "compress_type": "ZSTD"
    }
}
```

- name: the name of the inverted index, which must be specified in the query statement. The value cannot be summary.
- index_type: the type of the index. Set the value to PACK.
- term_payload_flag: specifies whether to store term_payload information (the payload of each term). The value 1 indicates that term_payload is stored. The value 0 indicates that term_payload is not stored. The values 1 and 0 of the following parameters have the same meaning. By default, term_payload is not stored.
- doc_payload_flag: specifies whether to store doc_payload information (the payload of each term in each document). By default, doc_payload is stored.
- position_list_flag: specifies whether to store position information. By default, the position information is stored. The configuration of position_payload_flag depends on the configuration of term_frequency_flag. Only if term_frequency_flag is set to 1, you can set position_list_flag to 1.
- position_payload_flag: specifies whether to store position_payload information (the payload of the term at each position in each document). By default, position_payload is not stored. The configuration of position_payload_flag depends on the configuration of position_list_flag. Only if position_list_flag is set to 1, you can set position_payload_flag to 1.
- term_frequency_flag: specifies whether to store the term frequency. By default, the term frequency is stored.
- term_frequency_bitmap: specifies whether to store the term frequency as a bitmap. The default value is 0. The configuration of term_frequency_bitmap depends on the configuration of term_frequency_flag. Only if term_frequency_flag is set to 1, you can set term_frequency_bitmap to 1.
- high_frequency_dictionary: the vocabulary that is used when you create a bitmap index. To create a bitmap index, specify this parameter. If you do not need to create a bitmap index, leave this parameter empty.
- high_frequency_adaptive_dictionary: the name of the rule that is used to create an adaptive bitmap index. To create an adaptive bitmap index, specify this parameter. If you do not need to create an adaptive bitmap index, leave this parameter empty.
- high_frequency_term_posting_type: the type of the bitmap index. If you set the parameter for creating a bitmap index or an adaptive bitmap index, you can set this parameter to both or bitmap to configure the type of the bitmap index. If you set this parameter to both, a bitmap index and an inverted index are created. If you set this parameter to bitmap, only a bitmap index is created. The default value is bitmap.
- index_fields: the fields on which you want to create an index. These fields must be of the TEXT type and use the same analyzer.
- boost: the weight of the field in an index. You can specify the name of the field on which you want to create the index and the boost value.
- index_analyzer: the analyzer that is used during the query. If you specify an analyzer, the analyzer is used to convert text to terms during the query. In this case, the analyzer can be inconsistent with the analyzers that are used in the fields. If you do not specify this parameter, the analyzers used in the fields are used. In this case, the analyzers that are used in the fields must be consistent. Take note that the analyzer can be only added to an index whose field type is TEXT.
- compress_type: You can configure one of ZSTD,SNAPPY,LZ4,LZ4HC, and ZLIB.
- format_version_id: specifies the version ID of the inverted index. The default value is 0, which indicates the inverted format of the AIOS benchmark that is migrated from indexlib. The optional value is 1. The default value is 0. The default value is 1. The default value is 1. The default value is 1. The default value is 1. The default value is 1. The default value is 1. The default value is 1. The default value is 1. The default value is 1.
#### Limits

- The index_name parameter cannot be set to summary.
- The fields in the PACK index must be of the TEXT type.
- If no analyzer is specified for the index, the analyzers specified for all fields in the same index must be the same.
- The order in which fields are configured must be the same as that in columns.
- If you set both the high_frequency_dictionary and high_frequency_adaptive_dictionary parameters, a bitmap index is created by using the high-frequency words specified in the high_frequency_dictionary parameter, regardless of the rule specified in the high_frequency_adaptive_dictionary parameter.
- You can specify the format_version_id parameter for all inverted indexes that are created on non-primary key columns. Before the offline phase starts, upgrade OpenSearch Vector Search Edition to the version that supports the corresponding format version online. Otherwise, the loading fails. The new version is supported online and formats of the old version can also be read.
- You can create a PACK index on a maximum of 32 fields.
### TEXT indexes
#### TEXT index introduction
A TEXT index is a single-field index that is created on fields of the TEXT type. An analyzer is used to convert text into multiple terms. A separate posting list is created for each term.  You can use truncation and high-frequency words bitmap and tfbitmap to improve retrieval performance.

| **Parameter** | **df** | **ttf** | **tf** | **fieldmap** | **position** | **positionpayload** | **docpayload** | **termpayload** |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Supported or not | Supported. | Optional | Optional | Not Supported. | Optional | Optional | Optional | Optional |

#### Example for configuring a TEXT index
```
{
    "name": "text_index",
    "index_type": "TEXT",
    "index_config": {
        "index_params": {
            "term_payload_flag": "1",
            "doc_payload_flag": "1",
            "position_payload_flag": "1",
            "position_list_flag": "1",
            "term_frequency_flag": "1"
        },
        "index_fields": [
            {
                "field_name": "title"
            }
        ],
        "compress_type": "ZSTD"
    }
}
```
In the text index configuration, index_name,index_type,term_payload_flag,doc_payload_flag,position_payload_flag,position_list_flag,term_frequency_flag, and compress_type have the same meaning, except that index_type must be set to TEXT and index_fields supports only one field.
#### Limits

- The index_name parameter cannot be set to summary.
### NUMBER indexes
#### Introduction to NUMBER indexes
A NUMBER index is a single-field index. The NUMBER index is an inverted index that is created for integer data of the INT8, UINT8, INT16, UINT16, INT32, UINT32, INT64, and UINT64 type.  The field on which the NUMBER index is created can be a multi-value field. A separate posting list is created for each value in the multi-value field.  A NUMBER index can store the following items.

| **Parameter** | **df** | **ttf** | **tf** | **fieldmap** | **position** | **positionpayload** | **docpayload** | **termpayload** |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Supported or not | Supported. | Optional | Optional | Not Supported. | Not Supported. | Not Supported. | Optional | Optional |

The following table describes the methods that can be used to improve the performance of NUMBER indexes.

| **Method name** | **Description** |
| --- | --- |
| Truncation | A separate inverted index is created for some high-quality documents based on your configuration. This index is retrieved first during retrieval. This prevents the system from retrieving unnecessary documents. The retrieval performance is doubled in the major retrieval. For more information, see Cluster configuration.  |
| High-frequency word bitmap | You can use bitmaps to store common high-frequency words. This helps reduce the space that is consumed by the index and improve retrieval performance. If you use bitmaps to store high-frequency words, only items ttf, df, termpayload, and docid can be retrieved. You can use bitmaps to store high-frequency words by configuring a high-frequency dictionary and an adaptive high-frequency dictionary. |
| tf bitmap | You can use bitmaps to store the term frequency information of each term in each document. You can use bitmaps to store the term frequency information of terms that have high document frequency. This way, no inverted index information is lost.  |

#### Configuration example of a NUMBER index
The following sample code provides an example on how to configure a NUMBER index in the schema.json file:
```
{
    "name": "number_index",
    "index_type": "NUMBER",
    "index_config": {
        "index_params": {
            "term_payload_flag": 0,
            "doc_payload_flag": 0,
            "term_frequency_flag": 0
        },
        "index_fields": [
            {
                "field_name": "number_field"
            }
        ],
        "compress_type": "ZSTD"
    }
}
```
The index_name,index_type,term_payload_flag,doc_payload_flag,term_frequency_flag, and compress_type parameters in the NUMBER index configuration have the same meanings, except that the index_type parameter must be set to NUMBER, and the index_fields parameter supports only one field, and the data type of the field must be integer.
Best practice: To reduce the index size, we recommend that you set term_payload_flag, doc_payload_flag, and term_frequency_flag to 0.
#### Limits

- The index_name parameter cannot be set to summary.
- A NUMBER index can be created for multiple integer values. When a NUMBER index is created, a separate inverted index is created for each value.
### STRING indexes
#### STRING index introduction
A STRING index is a single-field index. It is an inverted index that is created for data of the STRING type. Text is not converted into terms for fields of the STRING type. Each string value is used as a separate term for which a posting list is created. The fields on which a STRING index is created can be multivalued fields. Multiple field values can be separated by using delimiters. A posting list is created for each string value.  You can use truncation and high-frequency words bitmap and tfbitmap to improve retrieval performance.

| **Parameter** | **df** | **ttf** | **tf** | **fieldmap** | **position** | **positionpayload** | **docpayload** | **termpayload** |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Supported or not | Supported. | Optional | Optional | Not Supported. | Not Supported. | Not Supported. | Optional | Optional |

#### STRING index configuration example
```
{
    "name": "string_index",
    "index_type": "STRING",
    "index_config": {
        "index_params": {
            "term_payload_flag": "1",
            "doc_payload_flag": "1",
            "term_frequency_flag": "1"
        },
        "index_fields": [
            {
                "field_name": "user_name"
            }
        ],
        "compress_type": "ZSTD"
    }
}
```

- The following parameters have the same meaning in the configurations of STRING and PACK indexes: index_name, index_type, term_payload_flag, doc_payload_flag, term_frequency_flag, and file_compress. The exception is that index_type must be set to STRING, and the index_fields parameter supports only one field of the STRING type in the configuration of the STRING index. This field supports multiple integer values.

Best practice: To reduce the index size, we recommend that you set term_payload_flag, doc_payload_flag, and term_frequency_flag to 0.
#### Limits

- The index_name parameter cannot be set to summary.
- A STRING index can be created for multiple integer values. When a STRING index is created, a separate inverted index is created for each value.
### PRIMARYKEY64 indexes and PRIMARYKEY128 indexes
#### PRIMARYKEY INTRODUCTION
A PRIMARYKEY index is the primary key index of a document. You can configure only one PRIMARYKEY index. A PRIMARYKEY index supports all types of fields. It can store mappings between the hash values of index fields and the document IDs for removing duplicates. You can obtain the hash value for each document.

| **Parameter** | **df** | **ttf** | **tf** | **fieldmap** | **position** | **positionpayload** | **docpayload** | **termpayload** |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Supported or not | Physical backup files can be downloaded. | Not Supported. | Not Supported. | Not Supported. | Not Supported. | Not Supported. | Not Supported. | Not Supported. |

#### PRIMARYKEY configuration example
```
{
    "index_name": "primary_key_index",
    "index_type": "PRIMARY_KEY64",
    "index_config": {
        "index_fields": [
            {
                "field_name": "product_id"
            }
        ],
        "index_params": {
            "has_primary_key_attribute": "true",
            "pk_storage_type": "sort_array",
            "pk_hash_type": "default_hash"
        }
    }
}
```

- name: the name of the index.
- index_type: the type of the index. PRIMARY_KEY64 or PRIMARY_KEY128,64 or 128 indicates the number of bits in the hash. Generally, 64 bits are sufficient.
- index_fields: the field on which you want to create an index. Only one field is supported. All field types are supported. We recommend that you set this parameter to the field that corresponds to the primary key. You must specify column(field) with primary_key set to true.
- has_primary_key_attribute: The attribute of the primary key refers to the mapping between the document ID and the hash value of the primary key. If duplicates need to be removed in the query or the hash value of the primary key needs to be returned in the phase-1 query, the has_primary_key_attribute parameter must be specified. The default value of this parameter is false.
- pk_storage_type: specifies how the primary key is stored. Valid values are sort_array, hash_table, and block_array. The default value is sort_array.
   - sort_array: saves space.
   - hash_table: provides better performance.
   - block_array: allows you to configure the block cache and the mmap() function.
- pk_hash_type: the method to calculate the hash value for the primary key field. Valid values are default_hash, murmur_hash, and number_hash. The default value is default_hash.
   - default_hash: indicates the default hash method of strings.
   - murmur_hash: uses the MurmurHash function that can provide better performance.
   - number_hash: can be used when the primary key field is of the NUMBER type. This way, numbers are used to replace hash values. The resolution is faster than the hashing. However, numbers are more likely to be clustered than hash values.
#### Limits

- The PRIMARY_KEY64 and PRIMARY_KEY128 indexes support all types of fields.
- A PRIMARYKEY index is the primary key of a document. Therefore, you can configure at most one PRIMARYKEY index.
- PRIMARYKEY indexes do not support fields that contain null values.
- PRIMARYKEY indexes do not support compress_type file compression.
- The index_name parameter cannot be set to summary.
### RANGE indexes
#### Introduction to RANGE indexes
A RANGE index is created on integer values and is used to query documents in a specific range. When a RANGE index is used to replace the range filtering specified in a filter clause, the query performance is greatly improved. The more documents are filtered by using the filter clause, the more obvious the query performance is improved.
#### Configuration example
```json
{
    "name": "inputtime",
    "index_type": "RANGE",
    "index_config": {
        "index_fields": [
            {
                "field_name": "price"
            }
        ],
        "compress_type": "ZSTD"
    }
}
```
#### Query syntax
For more information about the query syntax of RANGE indexes, see RANGE query syntax.
#### Limits

- The index_fields of a range index can be INT64, INT32, UINT32, INT16, UINT16, INT8, or UINT8. Multi-value fields are not supported. Only one field can be specified
- RANGE indexes do not support fields that contain null values.
- The index_name parameter cannot be set to summary.
