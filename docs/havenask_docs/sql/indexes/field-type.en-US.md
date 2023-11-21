---
toc: content
order: 1
---

# Field types
| **Data type** | **Field Description** | **Supports Multi-value** | **Whether it can be used for forward indexes** | **Indicates whether the index can be used for a summary index.** | **Can I use an inverted index?** |
| --- | --- | --- | --- | --- | --- |
| TEXT | Stores text data. | Not granted | Not supported | Retained | Yes |
| STRING | Stores strings. | Retained | Retained | Retained | Yes |
| INT8 | Stores 8-bit signed integers. | Retained | Retained | Retained | Yes |
| UINT8 | Stores 8-bit unsigned integers. | Retained | Retained | Retained | Yes |
| INT16 | Stores 16-bit signed integers. | Retained | Retained | Retained | Yes |
| UINT16 | Stores 16-bit unsigned integers. | Retained | Retained | Retained | Yes |
| INT32 | Stores 32-bit signed integers. | Retained | Retained | Retained | Yes |
| UINT32 | Stores 32-bit unsigned integers. | Retained | Retained | Retained | Yes |
| INT64 | Stores 64-bit signed integers. | Retained | Retained | Retained | Yes |
| UINT64 | Stores 64-bit unsigned integers. | Retained | Retained | Retained | Yes |
| FLOAT | Stores single-precision 32-bit floating-point numbers. | Retained | Retained | Yes | Not granted |
| DOUBLE | Stores double-precision 64-bit floating-point numbers. | Retained | Retained | Yes | Not granted |
| RAW | Primitive data type for vector retrieval | Not granted | Not granted | Not supported | Yes |

- When you configure a schema, you must specify an analyzer for the TEXT field type. If you do not specify this parameter, the analyzer is simple_analyzer by default.
### configuration-methods
Example:
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
Additional information:
The name and type parameters are required. In addition, you must specify a column as the primary key. That is, you must specify "primary_key": true. The remaining configuration items are optional.

- **name**: the name of the field.
- **type**: the basic data type of the field. Valid values: TEXT,STRING,INT8,UINT8,INT16,UINT16,INT32, UINT32,INT64,UINT64,FLOAT,DOUBLE, and RAW. The string does not break the word, and the text does not break the word. RAW is a raw data field that is self-interpreted and used for vector indexing.
- **primary_key**: whether it is a primary key field, that is, whether it is a unique value field. Default value: false. Only one field can be configured as the primary key, and one field must be configured as true.
- **multi_value**: indicates that the field is multi-value. If the field is used to create an attirubte, a multi-value attribute is created. Default value: false.
- **separator**: For multi-value fields, specify the delimiter between multiple values. Default value: '^]'. The ASCII code is 0x1D.
- **analyzer**: the analyzer used for fields of the TEXT type.  If you do not configure a field whose field_type is TEXT, simple analyzer is used by default. Other types are not allowed. analyzer.
- **nullable**: Specifies whether the field allows null values. The default value is false. If enable_null is configured, the inverted __NULL__ can be used to recall documents with null values. You can query whether this field is null. If you do not specify this parameter, the numeric is 0 for a forward row, and the string and text types are empty.
- **default_value**: specifies a default value. For columns that do not exist in the document, you can specify a default value.
- **updatable**: indicates whether the forward row of the field can be updated. Valid values: INT8,UINT8,INT16,UINT16,INT32,UINT32,INT64,UINT64,FLOAT,DOUBLE, and STRING. If not configured, the default value is true for single-value non-string fields and false for multi-value fields.

