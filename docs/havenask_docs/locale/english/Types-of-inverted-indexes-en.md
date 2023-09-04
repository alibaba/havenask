

## PACK indexes

### Introduction to PACK indexes



A PACK index is a multi-field index that is created on fields of the TEXT type. Compared with a TEXT index, you can create the PACK index by merging multiple fields of the TEXT type for retrieval. The PACK index can also store section information so that information about the section where each search term is located can be queried.

You can use truncation and bitmaps and tfbitmaps of high-frequency words to improve retrieval performance.



| Item | df | ttf | tf | fieldmap | section information | position | positionpayload | docpayload | termpayload |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Item support | Supported | Optional | Optional | Not supported | Optional | Optional | Optional | Optional | Optional |



### Configuration example

```

{

	"index_name": "pack_index",                                      

	"index_type" : "PACK",                                      

	"term_payload_flag" : 1,                                           

	"doc_payload_flag" : 1,                                            

	"position_list_flag" : 1,                                          

	"position_payload_flag" : 1,                                       

	"term_frequency_flag" : 1,                                         

	"term_frequency_bitmap" : 1,                                       

	"has_section_attribute" : false,                                   

	"section_attribute_config":                                        

	{

		"has_section_weight":true,

		"has_field_id":true,

		"compress_type":"uniq|equal"

	},

	"high_frequency_dictionary" : "bitmap1",                           

	"high_frequency_adaptive_dictionary" : "df",                       

	"high_frequency_term_posting_type" : "both",                       

	"index_fields":                                                    

	[

		{"field_name":"subject", "boost":200000},                      

		{"field_name":"company_name", "boost":600000},

		{"field_name":"feature_value", "boost":600000},

		{"field_name":"summary", "boost":600000}

	],

	"index_analyzer" : "taobao_analyzer",                                      

    "file_compress":"simple_compress1",                                

    "format_version_id":1                                              

},

```



- index_name: the name of the inverted index. You must specify an index in the query statement. The index_name parameter cannot be set to summary.

- index_type: the type of the index. Set the value to PACK.

- term_payload_flag: specifies whether to store term_payload information. term_payload indicates the payload of each term. The value 1 indicates that term_payload is stored. The value 0 indicates that term_payload is not stored. In the following parameters, value 1 indicates the yes answer, while value 0 indicates the no answer. By default, term_payload is not stored.

- doc_payload_flag: specifies whether to store doc_payload information. doc_payload indicates the payload of each term in each document. By default, doc_payload is not stored.

- position_list_flag: specifies whether to store position information. By default, the position information is stored. The configuration of position_payload_flag depends on the configuration of term_frequency_flag. Only if term_frequency_flag is set to 1, you can set position_list_flag to 1.

- position_payload_flag: specifies whether to store position_payload information. position_payload indicates the payload of the term at each position in each document. By default, position_payload is not stored. The configuration of position_payload_flag depends on the configuration of position_list_flag. Only if position_list_flag is set to 1, you can set position_payload_flag to 1.

- term_frequency_flag: specifies whether to store the term frequency. By default, the term frequency is stored.

- term_frequency_bitmap: specifies whether to store the term frequency as a bitmap. The default value is 0. The configuration of term_frequency_bitmap depends on the configuration of term_frequency_flag. Only if term_frequency_flag is set to 1, you can set term_frequency_bitmap to 1.

- has_section_attribute: specifies whether to store section_attribute information. The default value is true. Text correlation can be calculated only after you set the has_section_attribute parameter to 1.

- section_attribute_config: the index configuration about section_attribute. The configuration takes effect when has_section_attribute is set to true. The has_field_id parameter specifies whether to store field_id information. The default value of the has_field_id parameter is true. If field_id information is not stored, the query process considers that all sections belong to the first field among the index fields. The has_section_weight parameter specifies whether to store weight information. The default value of the has_section_weight parameter is true. The compress_type parameter specifies the compression configuration method for section_attribute. By default, compression is disabled. The configuration method is the same as the method of configuring the compress_type parameter for multi-value attributes. If no section_attribute_config is displayed, the default configuration is used for all internal parameters by default.

- high_frequency_dictionary: specifies the vocabulary that is used when you create a bitmap index. This parameter is required for creating a bitmap index. If you do not need to create a bitmap index, you can leave this parameter empty.

- high_frequency_adaptive_dictionary: specifies the name of the rule that is used to create an adaptive bitmap index. This parameter is required for creating an adaptive bitmap index. If you do not need to create an adaptive bitmap index, leave this parameter empty.

- high_frequency_term_posting_type: specifies the type of the bitmap index. If you set the parameter for creating a bitmap index or an adaptive bitmap index, you can set this parameter to both or bitmap to configure the type of the index. If you set this parameter to both, the bitmap index and the inverted index are created. If you set this parameter to bitmap, only the bitmap index is created. The default value is bitmap.

- index_fields: specifies the fields on which you want to create the index. These fields must be of the TEXT type and use the same analyzer.

- boost: specifies the weight of the field in the index. You can specify the name of the field on which you want to create the index and the boost value.

- index_analyzer: the analyzer that is used during the query. If you specify an analyzer, the analyzer is used to convert text to terms during the query. In this case, the analyzer can be inconsistent with the analyzers that are used in the fields. If you do not set this parameter, the analyzers used in the fields are used to convert text to terms during the query. In this case, the same analyzer must be used in the fields. This parameter can be only configured for an index whose field type is TEXT.

- file_compress: the file compression method. Heaven Ask Engine V3 3.9.1 or later allows you to configure the file compression method. Set the file_compress parameter to an alias of a compression method to enable file compression for the inverted index. If you do not set this parameter, files are not compressed.

- format_version_id: specifies the version ID of the inverted index. The default value is 0, which indicates the format of the inverted index used when IndexLib is migrated to the AIOS of the benchmark version. For Havenask V3.9.1 or later, you can set this parameter to 1. Havenask V3.9.1 or later supports a series of optimization methods for the storage formats of the inverted index. The methods include the short URL VByte compression, optimized New PForDelta compression algorithm, and dictInline storage of continuous DocID intervals.



### Additional considerations



- The index_name parameter cannot be set to summary.

- The fields in the PACK index must be of the TEXT type.

- If no analyzer is specified for the index, the analyzers specified for all fields in the same index must be the same.

- The order in which fields are configured must be the same as the order in which fields are listed in index_fields.

- If you set the high_frequency_dictionary and high_frequency_adaptive_dictionary parameters at the same time, a bitmap index is created by using the high-frequency words specified in the high_frequency_dictionary parameter, regardless of the rule specified in the high_frequency_adaptive_dictionary parameter.

- You can specify the format_version_id parameter for all inverted indexes that are created on non-primary key columns. Before the offline phase starts, update Havenask online to the version that supports the corresponding format. Otherwise, the loading fails. The new version is supported online and formats of the old version can also be read.

- You can create a PACK index on a maximum of 32 fields.

## TEXT indexes

### Introduction to TEXT indexes



A TEXT index is a single-field index that is created on fields of the TEXT type. An analyzer is used to convert text into multiple terms. A separate posting list is created for each term.

You can use truncation and bitmaps and tfbitmaps of high-frequency words to improve retrieval performance.



| Item | df | ttf | tf | fieldmap | section information | position | positionpayload | docpayload | termpayload |

| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |

| Item support | Supported | Optional | Optional | Not supported | Not supported | Optional | Optional | Optional | Optional |





### Configuration example

```

{

	"index_name": "text_index",

	"index_type": "TEXT",

	"term_payload_flag" :  1 ,

	"doc_payload_flag" :  1 ,

	"position_payload_flag" : 1,

	"position_list_flag" : 1,

	"term_frequency_flag" : 1,

	"index_fields": "title",

  "file_compress":"simple_compress1"  

}

```

The following parameters have the same meanings in the configurations of TEXT and PACK indexes: index_name, index_type, term_payload_flag, doc_payload_flag, position_payload_flag, position_list_flag, term_frequency_flag, and file_compress. The exception is that index_type must be set to TEXT and the index_fields parameter supports only one field in the configuration of the TEXT index.

### Additional considerations



- The index_name parameter cannot be set to summary.

## NUMBER indexes

### Introduction to NUMBER indexes



A NUMBER index is a single-field index. The NUMBER index is an inverted index that is created for integer data of the INT8, UINT8, INT16, UINT16, INT32, UINT32, INT64, and UINT64 types.

The field on which the NUMBER index is created can be a multi-value field. A separate posting list is created for each value in the multi-value field.

The NUMBER index can store the following items.



| Item | df | ttf | tf | fieldmap | section information | position | positionpayload | docpayload | termpayload |

| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |

| Item support | Supported | Optional | Optional | Not supported | Not supported | Not supported | Not supported | Optional | Optional |





The following table describes the methods that can be used to improve the performance of NUMBER indexes.



| Method | Description |

| --- | --- |

| Truncation | A separate inverted index is created for some high-quality documents based on your configuration. This index is retrieved first during a query. This prevents the system from retrieving unnecessary documents. The retrieval performance is doubled, thanks to the optimized retrieval method. DO NOT TRANSLATE  |

| High-frequency word bitmap | You can use bitmaps to store common high-frequency words. This helps reduce the space that is occupied by the index and improve retrieval performance. If you use bitmaps to store high-frequency words, only items ttf, df, termpayload, and docid can be retrieved. You can use bitmaps to configure a high-frequency dictionary and an adaptive high-frequency dictionary that store high-frequency words. |

| tf bitmap | You can use bitmaps to store the term frequency information of each term in each document. You can use bitmaps to store the term frequency information of terms that have high document frequency. This way, no inverted index information is lost.  |





### Configuration example



The following sample code provides an example on how to configure a NUMBER index in the schema.json file:

```

{

	"index_name": "number_index",

	"index_type": "NUMBER",

	"term_payload_flag" :  0,

	"doc_payload_flag" :  0,

	"term_frequency_flag" : 0,

	"index_fields": "number_field",

  "file_compress":"simple_compress1"   

}

```

The following parameters have the same meanings in the configurations of NUMBER and PACK indexes: index_name, index_type, term_payload_flag, doc_payload_flag, term_frequency_flag, and file_compress. The exception is that index_type must be set to NUMBER, and the index_fields parameter supports only one field of the INTEGER type in the configuration of the NUMBER index.



Best practice: To reduce the index size, we recommend that you set term_payload_flag, doc_payload_flag, and term_frequency_flag to 0.

### Additional considerations



- The index_name parameter cannot be set to summary.

- A STRING index can be created for multiple integer values. When the STRING index is created, a separate inverted index is created for each value.

## STRING indexes

### Introduction to STRING indexes



A STRING index is a single-field index. The STRING index is an inverted index that is created for data of the STRING type. Text is not converted into terms for fields of the STRING type. Each string value is used as a separate term for which an inverted index is created. The fields on which a STRING index is created can be multi-value fields. Multiple field values can be separated with delimiters. An inverted index is created for each string value.

You can use truncation and bitmaps and tfbitmaps of high-frequency words to improve retrieval performance.



| Item | df | ttf | tf | fieldmap | section information | position | positionpayload | docpayload | termpayload |

| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |

| Item support | Supported | Optional | Optional | Not supported | Not supported | Not supported | Not supported | Optional | Optional |





### Configuration example

```

{

	"index_name": "string_index",

	"index_type": "STRING",

	"term_payload_flag" :  1,

	"doc_payload_flag" :  1,

	"term_frequency_flag" : 1,

	"index_fields": "user_name",

  "file_compress":"simple_compress1"   

}

```



- The following parameters have the same meanings in the configurations of STRING and PACK indexes: index_name, index_type, term_payload_flag, doc_payload_flag, term_frequency_flag, and file_compress. The exception is that index_type must be set to STRING, and the index_fields parameter supports only one field of the STRING type in the configuration of the STRING index. This field supports multiple integer values.



Best practice: To reduce the index size, we recommend that you set term_payload_flag, doc_payload_flag, and term_frequency_flag to 0.

### Additional considerations



- The index_name parameter cannot be set to summary.

- A STRING index can be created for multiple integer values. When the STRING index is created, a separate inverted index is created for each value.

## PRIMARYKEY64 or PRIMARYKEY128 indexes



### Introduction to PRIMARYKEY64 indexes and PRIMARYKEY128 indexes



The PRIMARYKEY index is the primary key index of a document. You can configure only one PRIMARYKEY index. The PRIMARYKEY index supports all types of fields. The PRIMARYKEY index can store mappings between the hash values of index fields and the document IDs for the deduplication purpose. You can obtain the hash value for each document.



| Item | df | ttf | tf | fieldmap | section information | position | positionpayload | docpayload | termpayload |

| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |

| Item support | Supported | Not supported | Not supported | Not supported | Not supported | Not supported | Not supported | Not supported | Not supported |





### Configuration example

```

{

	"index_name": "primary_key_index",

	"index_type" : "PRIMARYKEY64",

	"index_fields": "product_id",

	"has_primary_key_attribute": true,

	"is_primary_key_sorted": true,

  "pk_storage_type" : "sort_array",

  "pk_hash_type" : "default_hash"

}

```



- index_name: the name of the index.

- index_type: the type of the index. You can set the index_type parameter to PRIMARYKEY64 or PRIMARYKEY128. The numbers 64 and 128 indicate bits of hash values. In most cases, 64 bits are sufficient.

- index_fields: specifies the field on which you want to create the index. Only one field is supported. All field types are supported. We recommend that you set this parameter to the field that corresponds to the primary key.

- has_primary_key_attribute: The attribute of the primary key refers to the mapping between the document ID and the hash value of the primary key. If duplicates need to be removed in the query or the hash value of the primary key needs to be returned in the phase-1 query, the has_primary_key_attribute parameter must be specified. The default value of this parameter is false.

- is_primary_key_sorted: specifies whether the PRIMARYKEY index is optimized. If this parameter is set to true, the indexes that are dumped out are sorted by primary key. This accelerates the query. The default value is false.

- pk_storage_type: specifies how the primary key is stored. Valid values: sort_array, hash_table, and block_array. The default value is sort_array.

   - sort_array: saves space.

   - hash_table: provides better performance.

   - block_array: allows you to configure the block cache and the mmap() function.

- pk_hash_type: specifies the method used to calculate the hash value for the primary key field. Valid values: default_hash, murmur_hash, and number_hash. The default value is default_hash.

   - default_hash: indicates the default hash method of strings.

   - murmur_hash: uses the MurmurHash function and provides better performance.

   - number_hash: can be used when the primary key field is of the NUMBER type. This way, numbers are used to replace hash values. The resolution is faster than hashing. However, numbers are more likely to be clustered than hash values.

### Additional considerations



- The PRIMARYKEY64 or PRIMARYKEY128 index supports all types of fields.

- The PRIMARYKEY index is the primary key of a document. Therefore, you can configure only one PRIMARYKEY index.

- PRIMARYKEY indexes do not support fields that contain null values.

- PRIMARYKEY indexes do not allow you to set the file_compress parameter to enable file compression.

- The index_name parameter cannot be set to summary.

## DATE indexes

### Introduction to DATE indexes

A DATE index is created on date and time values and is used to query a time range.

### Configuration example

```json

"fileds":

[

    {"field_name":"inputtime",     "field_type":"UINT64", "binary_field": false},

    ...

]

...

"indexs":

[

    {

        "index_name": "inputtime",                                        1

        "index_type" : "DATE",                                            2

        "index_fields": "inputtime",                                      3

        "build_granularity": "minute",                                    4

        "file_compress":"simple_compress1"                                5

    },

    ...

]

```



- index_name: the name of the inverted index. You must specify an index in the query statement.

- index_type: the type of the index. Set the value to DATE.

- index_fields: specifies the field on which you want to create the index.  You can set the field_type parameter to UINT64, DATE, TIME, or TIMESTAMP to create a DATE index.

- build_granularity: the granularity at which the term dictionary is built. If you set this parameter to minute, values that are expressed in seconds or microseconds in the data are converted to the invalid value 0. Only values that are expressed in minutes can be queried.

- file_compress: specifies that the postings file is compressed. For more information, see the "PACK indexes" section in this topic.



### Query syntax

For information about the query syntax of DATE indexes, see "Query syntax of DATE indexes".

### Additional considerations



- DATE indexes do not support bitmaps.

- The term dictionary can be built at one of the following granularities: year, month, day, hour, minute, second, and millisecond. If you set the time granularity to a small unit such as millisecond, , a large amount of storage space is required.

- DATE, TIME, and TIMESTAMP values are converted to timestamps in milliseconds that have elapsed since 00:00:00 00.000, January 1, 1970 based on their formats. Timestamps in Greenwich Mean Time (GMT) and Coordinated Universal Time (UTC) are supported regardless of time zones. An inverted index is created based on the timestamps. You also specify the timestamps that indicate the query time range based on the corresponding criteria.

- You can leave the term parameter empty in a query. In this case, the records about fields for which enable_null is set to true can be returned.

- The index_name parameter cannot be set to summary.



## RANGE indexes

### Introduction to RANGE indexes

A RANGE index is created on integer values and is used to query documents in a specific range. When the RANGE index is used to replace the range filter specified in the FILTER clause, the query performance is improved. When you want to filter a large number of documents, the RANGE index provides significant improvement in the query performance compared with the scenario in which you use the FILTER clause.

### Configuration example

```json

"fileds":

[

    {"field_name":"price",     "field_type":"INT64", "binary_field": false},

    ...

]

"indexs":

[

    {

        "index_name": "inputtime",

        "index_type" : "RANGE",

        "index_fields": "price",

        "file_compress":"simple_compress1"  

    },

    ...

]

```

### Query syntax

For information about the query syntax of RANGE indexes, see "Query syntax of RANGE indexes".

### Additional considerations



- You can set the field_type parameter to INT64, INT32, UINT32, INT16, UINT16, INT8, or UINT8 to create a RANGE index. Multi-value fields are not supported.

- RANGE indexes do not support fields that contain null values.

- The index_name parameter cannot be set to summary.

## SPATIAL indexes

### Introduction to SPATIAL indexes

A SPATIAL index is created for the longitudes and latitudes of given points and is used for geospatial queries, including point range queries, line queries, and polygon queries.

### Configuration example

```json

"fileds":

[

    {"field_name":"location",         "field_type":"LOCATION"},

    {"field_name":"line",     "field_type":"LINE"},

    {"field_name":"polygon",     "field_type":"POLYGON"},

    ...

]

....

"indexs":

[

    {

        "index_name": "inputtime",                                        1

        "index_type" : "SPATIAL",                                         2

        "index_fields": "location",                                       3

	    	"max_search_dist": 10000,                                         4

	    	"max_dist_err": 20                                                5

	    	"distance_loss_accuracy":0.025,                                   6

        "file_compress":"simple_compress1"                                7

    },

    ...

]

```



- index_name: the name of the inverted index. You must specify an index in the query statement.

- index_type: the type of the index. Set the value to SPATIAL.

- index_fields: specifies the fields on which you want to create the index. The field types must be LOCATION, LINE, and POLYGON.

   - LOCATION: You can specify values for fields of the LOCATION data type in the location={Longitude} {Latitude} format, for example, location=116 40.

   - LINE: You can specify values for fields of the LINE data type in the line=location,location,location...^]location,location... format, for example, line=116 40,117 41,118 42^].... If you use the Engine SDK to push data, specify the LINE field in the following format: line: ["location,location,location...", "location,location,location..."]

   - If you push a field of the POLYGON data type to the Engine, use the following format: polygon=location1,location2,...location1^]... If you use the Engine SDK to push a field of the POLYGON data type to the Engine, use the following format: line : [["location,location,location",...]. A polygon can be a convex polygon or a concave polygon. The start and end points of the polygon must be the same. The two adjacent edges cannot be collinear. The edges of the polygon cannot be self-intersecting.

- max_search_dist: specifies the maximum distance (diameter) that is covered during the query. Unit: meters. The value of the max_search_dist parameter must be greater than the value of the max_dist_err parameter.

- max_dist_err: specifies the deviation in the maximum distance (diameter) when the term dictionary is built. Unit: meters. The minimum value of this parameter is 0.05.

- distance_loss_accuracy: specifies the loss of precision. The engine improves the performance of polyline and polygon queries at the cost of precision loss. Performance improvement method: Range of the distance to the outermost layer = Diagonal length of the circumscribed rectangle of the polyline or polygon x Value of the distance_loss_accuracy parameter. The default value of the distance_loss_accuracy parameter is 0.025.

- For information about the file compression method, see the "PACK indexes" section.

### Query syntax

For information about the query syntax of SPATIAL indexes, see "Query syntax of SPATIAL indexes".

### Additional considerations



- The point coordinates of the lines and polygons are mapped to a flat world map to determine the scope of the line queries and polygon queries. The scenarios in which the query scope crosses the 180 degrees longitude are not considered. The query result of the inverted index on the location field is accurate. The query results of the inverted index on the line and polygon fields must be filtered.

- The index_name parameter cannot be set to summary.


