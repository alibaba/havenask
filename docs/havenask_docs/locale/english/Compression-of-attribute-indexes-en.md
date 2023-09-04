## Multi-value attribute deduplication

By default, the values of multi-value attributes are sorted by document order. The offsets of multi-value attributes are sorted in ascending order. In actual business scenarios, a large number of documents correspond to duplicate values of attributes. If you enable multi-value attribute deduplication, duplicate values of indexes can be removed. This way, the size of generated indexes can be reduced.

Multi-value attribute deduplication can be used to deduplicate multi-value attributes, single-value fields whose data type is STRING, and section attributes.

When the system performs deduplication, memory overheads increase to perform build operations and merge operations because data structures are used, including dictionaries for data deduplication. We recommend that you do not enable multi-value attribute deduplication if the duplication rate is low.

## Equal-value compression

In actual business scenarios, after the offset values of single-value attributes and multi-value attributes are globally sorted based on a specific field, duplicate values often appear in sequence. If you compress the duplicate values, large storage space is saved. Equal-value compression uses a smaller of bits to store the duplicate values. This way, you can compress the corresponding indexes.

Equal-value compression is used for the offset files of single-value attributes and multi-value attributes. You can enable multi-value attribute deduplication and equal-value compression at the same time for multi-value attributes, single-value attributes whose data type is STRING, and section attributes.

## Self-adaptation storage for offset files

The number of multi-value attributes is equal to the number of offset files that are generated because each multi-value attribute is stored in an isolated location. If the system uses 8 bits to store each offset file, storage overheads are high. To solve this issue, Havenask supports self-adaptation storage for offset files. If the size of all offset files is less than 4 GB, the system uses 4 bits to store the offset files.

You do not need to set this parameter.

## Sample code

```json

{

  ...

    "fields":[

        {

            "field_name":"category",

            "field_type":"INTEGER",

            "multi_value":true,

            "compress_type":"uniq|equal"

        },

        {

            "field_name":"price",

            "field_type":"INTEGER",

            "user_defined_param":{

                "key":"hello"

            }

        }

    ]

  ...

}

```



- multi_value: specifies whether the field is a multi-value field. Valid values: true and false. Default value: false.

- compress_type: specifies the compression method that you want to use when the field is stored as an attribute. By default, the value of the compress_type parameter is set to an empty string. if the value is set to an empty string, your forward index is not compressed. Valid values:

   - uniq: If you want to perform multi-value attribute deduplication, set the value of compress_type to uniq.

   - equal: If you want to perform equal-value compression, set the value of compress_type to equal.

   - patch_compress: If you want to perform patch-file-based compression, set the value of compress_type to patch_compress.

   - You can specify multiple values for compress_type and separate the values with vertical bars (|). For example, you can set the value of compress_type to uniq | equal.

   - If you want to compress a multi-value attribute of the FLOAT data type, you can set the value of compress_type to block_fp, fp16, or int8#N. N is a value that specifies the value range of your FLOAT data. The value range is from minus N to positive N.

   - If you want to compress a single-value attribute of the FLOAT data type, you can set the value of compress_type to fp16 or int8#N. Take note that you cannot use fp16 or int8#N together with uniq | equal for a single-value attribute that contains data of the FLOAT type.


