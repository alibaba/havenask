---
toc: content
order: 4
---

# 倒排索引

<div class="lake-content" typography="classic"><h2 id="gD4Vi" style="font-size: 24px; line-height: 32px; margin: 21px 0 5px 0"><span class="ne-text">什么是倒排索引</span></h2><p id="u291aeb99" class="ne-p" style="margin: 0; padding: 0; min-height: 24px"><span class="ne-text"><br /></span><span class="ne-text">倒排索引也常被称为反向索引、置入档案或反向档案，是一种索引方法，被用来存储在全文搜索下某个单词在一个文档或者一组文档中的存储位置的映射。它是文档检索系统中最常用的数据结构。 <br /></span><span class="ne-text">通过倒排索引，可以快速定位单词所在的文档列表以及该词在文档中的位置，词频等信息。供信息分析使用。 </span></p><h2 id="IQhVR" style="font-size: 24px; line-height: 32px; margin: 21px 0 5px 0"><span class="ne-text">倒排索引存储信息</span></h2>

信息名称 | 描述
-- | --
ttf | 全称：total term frequency, 表示检索词在所有文档中出现的总次数
df | 全称：document frequency, 表示包含检索词的文档总数
tf | 全称：term frequency, 表示检索词在文档中出现的次数
docid | 全称：document id, 是文档在引擎中的唯一标识，可以通过docid获取到原文档的其他信息。
fieldmap | 全称：field map, 用于记录包含检索词的field信息
section 信息 | 用户可以为某些文档分段，然后为每一段添加附属信息。该信息可以在检索时取出，供后续处理使用
position | 用于记录检索词在文档中的位置信息
positionpayload | 全称：position payload, 用户可以为文档不同位置设置payload信息，并可以在检索时取出，供后续处理用
docpayload | 全称：document payload, 用户可以为某些文档添加附属信息，并可以在检索时取出，供后续处理使用
termpayload | 全称：term payload, 用户可以为某些词添加附属信息，并可以在检索时取出，供后续处理使用

<p id="uebc11eff" class="ne-p" style="margin: 0; padding: 0; min-height: 24px"><br></p><h2 id="ROOg1" style="font-size: 24px; line-height: 32px; margin: 21px 0 5px 0"><span class="ne-text">倒排索引检索的基本流程</span></h2><p id="uefab12c0" class="ne-p" style="margin: 0; padding: 0; min-height: 24px"><span class="ne-text"><br /></span><span class="ne-text">当用户查询单词M的倒排索引时，首先引擎会查询词典文件，找到索引词在倒排索引文件(posting文件)的起始位置。<br /></span><span class="ne-text">随后引擎通过解析倒排链，获取词M存储在倒排链的三部分信息：TermMeta,DocList, PositionList。<br /></span><span class="ne-text">TermMeta存储的是对索引词的基本描述，主要包括词的df、ttf、termpayload信息。<br /></span><span class="ne-text">DocList包含索引词的文档信息列表，文档信息包括:DocumentId,文档中的检索词频(tf), docpayload, 包含检索词的field信息(termfield)。<br /></span><span class="ne-text">PositionList是检索词在文档中的位置信息列表，主要包括检索词在文档中的具体位置(position)和positionpayload信息。 </span></p><p id="u959f4b30" class="ne-p" style="margin: 0; padding: 0; min-height: 24px"><br></p><p id="u40689884" class="ne-p" style="margin: 0; padding: 0; min-height: 24px"><img src="https://cdn.nlark.com/lark/0/2018/png/114751/1541056605570-e4f5b3f9-0b89-4fc5-9949-b31c2355621f.png" width="546" id="flyif" class="ne-image"></p></div>


### 配置示例
name、index_type、index_fields为必须配置的项，其余为可选配置。
对于PACK索引，index_fields可以指定一个或多个字段，其他倒排索引类型，index_fields均只能指定一个字段。
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
### PACK 索引
#### PACK索引介绍
PACK索引是多字段索引。对TEXT类型的字段建立索引。与TEXT索引相比，该索引将多个TEXT字段合并，建立一个索引，共同检索；该索引还可存储段落信息，用于查询每一个检索词所在的段落及相关信息。 可以采用截断，高频词bitmap和tfbitmap的方式提高检索性能 .

| **信息名称** | **df** | **ttf** | **tf** | **fieldmap** | **position** | **positionpayload** | **docpayload** | **termpayload** |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 是否支持 | 支持 | 可选 | 可选 | 不支持 | 可选 | 可选 | 可选 | 可选 |

#### pack索引配置示例
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

- name：倒排索引名字，在查询语句中需要指定索引查询，不能为summary。
- index_type：索引的类型，必须为PACK。
- term_payload_flag：是否需要存储term_payload（每个term的payload），1表示存储，0表示不存储(以下的1, 0都表示这个含义），默认不存储。
- doc_payload_flag：是否需要存储doc_payload（在每篇Document中每个term的payload），默认值不存储。
- position_list_flag：是否需要存储position信息，默认存储。配置依赖 : term_frequency_flag=1 (只有在配置了term_frequency_flag=1 的情况下， 才可以选择配置position_list_flag=1)。
- position_payload_flag：是否需要存储position_payload（在每篇Document中的每个位置上的term的payload），默认不存储。配置依赖： position_list_flag=1(只有在配置了 position_list_flag=1 的情况下，才可以配置 position_payload_flag = 1)。
- term_frequency_flag：是否需要存储term frequency, 默认存储。
- term_frequency_bitmap：是否需要将term frequency存储为bitmap的形式，默认为0。配置依赖：term_frequency_flag=1 (只有配置了term_frequency_flag=1的情况下，才可以配置 term_frequency_bitmap=1)。
- high_frequency_dictionary：如果配置此项则表示需要建立bitmap索引，此处指定了建立bitmap索引时用到的词表，不需要bitmap索引则可以不配置此项。
- high_frequency_adaptive_dictionary：如果配置此项则表示需要建立自适应bitmap索引，此处指定了建立自适应bitmap索引的规则名称，不需要建立自适应bitmap索引则可以不配置此项。
- high_frequency_term_posting_type：如果配置了bitmap索引或者自适应bitmap索引，则可以配置bitmap索引的类型（both/bitmap）。如果配置成both，表示既有bitmap，也有倒排索引。否则只有bitmap信息，默认值是bitmap。
- index_fields：表示需要进入该index的field，field必须为TEXT类型，而且需要使用相同的分析器。
- boost：配置建立索引的field的名字以及该field在这个索引中的权重（boost的值）。
- index_analyzer：配置查询过程中的分词器。如果配置了该分词器，查询过程中就采用该分词器进行分词，此时该分词器可以和field中的分词器不一致。如果不配置该项，则采用field中的分词器，此时要求field中分词器保持一致。注意该分词器只能在field类型为text的索引中添加。
- compress_type：可配置ZSTD,SNAPPY,LZ4,LZ4HC,ZLIB中的一种
- format_version_id：指定倒排索引的版本id，默认为0（代表indexlib迁移aios基准版本的倒排格式），可选设置为1（支持一系列倒排存储格式优化，包括：短链vByte压缩、newPForDelta压缩算法优化、连续docid区间dictInline存储）
#### 注意事项

- index_name 不允许命名为"summary"。
- PACK索引的field必须都为TEXT类型。
- 在没有为index指定分词器的情况下，配置在同一个索引里的所有field指定的分词器必须一样。
- 此处配置field的顺序必须与columns中顺序保持一致。
- 如果同时配置了high_frequency_dictionary和high_frequency_adaptive_dictionary，high_frequency_dictionary中定义的高频词直接生成bitmap索引，不受high_frequency_adaptive_dictionary的规则限制。
- format_version_id支持对非pk类型的所有倒排开启，离线开启前需要在线升级到支持对应version的版本，否则会加载失败。在线支持新版本的同时可以兼容读取老版本格式。
- pack索引最多支持32个字段。
### TEXT索引
#### TEXT索引介绍
TEXT索引是单字段索引。用于对TEXT类型的字段建立索引。采用分词器将TEXT切分成多个检索词(term)，并对每一个词单独建立倒排链。 可以采用截断，高频词bitmap和tfbitmap的方式提高检索性能

| **信息名称** | **df** | **ttf** | **tf** | **fieldmap** | **position** | **positionpayload** | **docpayload** | **termpayload** |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 是否支持 | 支持 | 可选 | 可选 | 不支持 | 可选 | 可选 | 可选 | 可选 |

#### TEXT索引配置示例
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
TEXT索引配置中的index_name，index_type，term_payload_flag，doc_payload_flag，position_payload_flag，position_list_flag，term_frequency_flag, compress_type的含义相同，只是index_type必须为TEXT，并且index_fields只支持一个字段。
#### 注意事项

- index_name 不允许命名为"summary"。
### NUMBER 索引
#### NUMBER索引介绍
NUMBER索引是单字段索引。用于对整型(INT8、UINT8、INT16、UINT16、INT32、UINT32、INT64、UINT64)数据建立倒排索引。 建立NUMBER索引的字段可以是多值字段，多值字段中的每一个数值都会单独建立倒排链。 NUMBER索引可存储的信息如下：

| **信息名称** | **df** | **ttf** | **tf** | **fieldmap** | **position** | **positionpayload** | **docpayload** | **termpayload** |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 是否支持 | 支持 | 可选 | 可选 | 不支持 | 不支持 | 不支持 | 可选 | 可选 |

改善NUMBER倒排索引性能的方法如下：

| **方法名称** | **描述** |
| --- | --- |
| 截断 | 可以根据配置，将一些优质文档单独建倒排索引，检索时优先查找该部分索引。减少对不必要文档的检索过程。在主搜中性能提升一倍左右。具体的配置方法参考：集群配置link。 |
| 高频词的bitmap | 对于常见的高频词，可以采用bitmap索引来存储，减少索引空间大小，提高检索性能。采用bitmap存储高频词只能检索到ttf, df, termpayload, docid信息。用户可以通过设置高频词典和自适应高频词典来设定采用bitmap结构存储高频词 |
| tf bitmap | 采用bitmap方式存储每篇doc的tf信息。对于df数大的词，可以采用bitmap的方式存储其tf信息。采用该方式存储无倒排索引信息丢失。 |

#### NUMBER索引配置示例
在schema.json文件中，number索引的配置示例如下：
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
NUMBER索引配置中的index_name，index_type，term_payload_flag，doc_payload_flag，term_frequency_flag, compress_type的含义相同，只是index_type必须为NUMBER，并且index_fields只支持一个字段，并且字段的类型必须为整型。
最佳实践：为了减小索引大小，建议将term_payload_flag、doc_payload_flag、term_frequency_flag设置为0。
#### 注意事项

- index_name 不允许命名为"summary"。
- 支持对多值整型构建索引，构建索引时每个值都会建独立的倒排索引。
### STRING 索引
#### STRING索引介绍
STRING索引是单字段索引。用于对STRING类型的数据建立倒排索引。STRING字段不采用分词处理，每一个STRING都作为一个单独的索引词建立倒排链。STRING索引的字段允许为多值字段，按照多值分隔符切分，对每个STRING建立倒排。 可以采用截断，高频词bitmap和tfbitmap的方式提高检索性能

| **信息名称** | **df** | **ttf** | **tf** | **fieldmap** | **position** | **positionpayload** | **docpayload** | **termpayload** |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 是否支持 | 支持 | 可选 | 可选 | 不支持 | 不支持 | 不支持 | 可选 | 可选 |

#### STRING索引配置示例
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

- STRING索引配置中的index_name，index_type，term_payload_flag，doc_payload_flag，term_frequency_flag, file_compress的含义相同，只是index_type必须为STRING，并且index_fields只支持一个字段，并且字段的类型必须为STRING，支持多值整型字段。

最佳实践：为了减小索引大小，建议将term_payload_flag、doc_payload_flag、term_frequency_flag设置为0。
#### 注意事项

- index_name 不允许命名为"summary"。
- 支持对多值整型构建索引，构建索引时每个值都会建独立的倒排索引。
### PRIMARYKEY64/PRIMARYKEY128 索引
#### PRIMARYKEY介绍
PRIMARYKEY索引是文档的主键索引，只能配置一个。该索引支持所有类型的字段。该索引可以存储索引字段hash值与docid的映射关系，用于排重使用。用户可以获取每个doc的hash值

| **信息名称** | **df** | **ttf** | **tf** | **fieldmap** | **position** | **positionpayload** | **docpayload** | **termpayload** |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 是否支持 | 支持 | 不支持 | 不支持 | 不支持 | 不支持 | 不支持 | 不支持 | 不支持 |

#### PRIMARYKEY配置示例
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

- name：索引名称。
- index_type：索引类型，PRIMARY_KEY64或者PRIMARY_KEY128，64和128表示hash的位数，一般64位就足够了。
- index_fields：构建索引的字段，只支持一个字段，任何字段类型都可以，建议设置为主键对应的字段。必须指定为primary_key为true的column（field）。
- has_primary_key_attribute：primarykey attribute是指doc id到primarykey hash值的映射，如果查询时需要去重或者需要一阶段查询时返回PK的hash值，则必须配置，此选项默认值为false。
- pk_storage_type 配置pk的存储方式, 可选 sort_array、 hash_table、block_array。默认sort_array
   - sort_array更省空间,
   - hash_table性能更好
   - block_array支持配置block cache和mmap
- pk_hash_type配置pk字段计算hash值的对应方法，可选default_hash、murmur_hash和number_hash。默认是default_hash
   - default_hash默认的字符串hash方法
   - murmur_hash采用murmur哈希算法（性能更好）
   - number_hash当pk field为number类型时可以开启，直接用number数值代替hash值（转换性能比hash更快，但是使用原值后对比hash值可能更容易出现簇拥
#### 注意事项

- PRIMARY_KEY64/PRIMARY_KEY128索引支持所有类型的field。
- PRIMARYKEY索引是文档的主键，因此PRIMARYKEY索引至多配置1个。
- PRIMARYKEY索引不支持空值字段。
- PRIMARYKEY索引不支持compress_type文件压缩。
- index_name 不允许命名为"summary"。
### RANGE索引
#### RANGE索引介绍
range索引对整型数据构建索引，用于查询某一范围的文档，用于替代filter子句中的范围过滤时，性能会有较大的提升（filter过滤掉的文档越多，性能提升越明显）。
#### 配置示例
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
#### 查询语法
RANGE索引的查询语法请参考RANGE查询语法。
#### 注意事项

- range索引的index_fields可以为INT64, INT32, UINT32, INT16, UINT16, INT8, UINT8，不支持多值字段.只能指定一个字段
- range索引不支持空值字段。
- index_name 不允许命名为"summary"。
