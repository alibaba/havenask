---
toc: content
order: 7
---


# Vector indexes

Vector retrieval refers to the expression of images or content in the form of vectors and the creation of a vector index library. You can enter one or more vectors during queries to return the top K approximate results based on the vector distance.   
Heaven Ask Engine is deeply integrated with the [proxima](https://github.com/alibaba/proxima) core library, so that the Heaven Ask Engine has the ability of vector retrieval. Heaven Ask Engine supports multiple mainstream vector index types, such as graph indexes and cluster indexes, and features high performance and low cost. Taking hnsw index as an example, on the sift 1M and deep 10M public test sets, using 16core 64 GB machines, the vector index of the sky-asking engine can reach thousands of qps and the latency within 10ms. At the same time, the vector index of Heaven Ask Engine can be loaded in non-full memory mode, which can greatly reduce the overhead of machine memory, increase the scale of stand-alone index, and reduce machine costs.

For more information about examples of vector retrieval, see [Vector examples](https://github.com/alibaba/havenask/tree/main/example/cases/vector).

## Configure a vector index without categories
```
{
    "columns": [
        {
            "name": "id",
            "type": "UINT32"
        },
        {
            "name": "vector",
            "type": "STRING"
        },
        {
            "name": "DUP_vector",
            "type": "RAW"
        }
    ],
    "indexes": [
        {
            "name": "vector_index",
            "index_type": "ANN",
            "index_config": {
                "index_fields": [
                    {
                        "boost": 1,
                        "field_name": "id"
                    },
                    {
                        "boost": 1,
                        "field_name": "DUP_vector"
                    }
                ],
                "index_params": {
                    "indexer": "aitheta2_indexer",
                    "parameters": "{\"enable_rt_build\":\"true\",\"min_scan_doc_cnt\":\"20000\",\"vector_index_type\":\"HNSW\",\"major_order\":\"col\",\"builder_name\":\"HnswBuilder\",\"distance_type\":\"SquaredEuclidean\",\"embedding_delimiter\":\",\",\"enable_recall_report\":\"true\",\"is_embedding_saved\":\"true\",\"linear_build_threshold\":\"5000\",\"dimension\":\"128\",\"search_index_params\":\"{\\\"proxima.hnsw.searcher.ef\\\":500}\",\"searcher_name\":\"HnswSearcher\",\"build_index_params\":\"{\\\"proxima.hnsw.builder.max_neighbor_count\\\":100,\\\"proxima.hnsw.builder.efconstruction\\\":500,\\\"proxima.hnsw.builder.thread_count\\\":20}\"}"
                }
            }
        }
    ]
}
```
## Configure a vector index with categories
Categories are introduced to allow you to search for vectors based on categories. For example, an image belongs to different categories. If you do not build a vector index with categories and filter only the retrieved vectors, no results may be returned. You must specify the category that you want to query. If you do not specify the category, the engine queries all categories and the performance decreases sharply.
```
{
    "columns": [
        {
            "name": "id",
            "type": "UINT32"
        },
        {
            "name": "category",
            "type": "UINT32"
        },
        {
            "name": "vector",
            "type": "STRING"
        },
        {
            "name": "DUP_vector",
            "type": "RAW"
        }
    ],
    "indexes": [
        {
            "name": "vector_index",
            "index_type": "ANN",
            "index_config": {
                "index_fields": [
                    {
                        "boost": 1,
                        "field_name": "id"
                    },
                    {
                        "boost": 1,
                        "field_name": "category"
                    },
                    {
                        "boost": 1,
                        "field_name": "DUP_vector"
                    }
                ],
                "index_params": {
                    "indexer": "aitheta2_indexer",
                    "parameters": "{\"enable_rt_build\":\"true\",\"min_scan_doc_cnt\":\"20000\",\"vector_index_type\":\"HNSW\",\"major_order\":\"col\",\"builder_name\":\"HnswBuilder\",\"distance_type\":\"SquaredEuclidean\",\"embedding_delimiter\":\",\",\"enable_recall_report\":\"true\",\"is_embedding_saved\":\"true\",\"linear_build_threshold\":\"5000\",\"dimension\":\"128\",\"search_index_params\":\"{\\\"proxima.hnsw.searcher.ef\\\":500}\",\"searcher_name\":\"HnswSearcher\",\"build_index_params\":\"{\\\"proxima.hnsw.builder.max_neighbor_count\\\":100,\\\"proxima.hnsw.builder.efconstruction\\\":500,\\\"proxima.hnsw.builder.thread_count\\\":20}\"}"
                }
            }
        }
    ]
}
```

- findex_fields: the field used to build a vector index. The field must be of the RAW type and must contain at least two fields. One field is the primary key (or the hash value of the primary key). The field value must be an integer. The other field contains a vector. If you need to build a vector index based on categories, you can add a category field. The field type is RAW and the field value is of the INTEGER data type. The order of the fields in the index parameter must be configured in the same way as that in the fields parameter. If the category field exists, the order must be the primary key field, the category field, and the vector field. You can use the built-in DupFieldProcessor to copy RAW fields from the source fields to the destination fields. You do not need to add RAW fields to the data. For more information, see [Configure vector data processing](https://github.com/alibaba/havenask/tree/main/hape/example/cases/vector/vector_conf/cluster_templates/havenask/direct_table/data_tables/default_table.json).
- name: the name of the vector index.
- index_type: the type of the index. It must be an ANN.
- indexer: the plug-in that you want to use to build the vector index. Set the value to aitheta2_indexer.
- parameters: the construction and query parameters of a vector index. The parameters vary with the index type.


## Detailed configuration of vector indexes
```
{
    "dimension": "128",
    "embedding_delimiter": ",",
    "builder_name": "QcBuilder",
    "searcher_name": "QcSearcher",
    "distance_type": "SquaredEuclidean",
    "search_index_params": "{\"proxima.qc.searcher.scan_ratio\":0.01}",
    "build_index_params": "{\"proxima.qc.builder.quantizer_class\":\"Int8QuantizerConverter\",\"proxima.qc.builder.quantize_by_centroid\":true,\"proxima.qc.builder.optimizer_class\":\"BruteForceBuilder\",\"proxima.qc.builder.thread_count\":10,\"proxima.qc.builder.optimizer_params\":{\"proxima.linear.builder.column_major_order\":true},\"proxima.qc.builder.store_original_features\":false,\"proxima.qc.builder.train_sample_count\":3000000,\"proxima.qc.builder.train_sample_ratio\":0.5}",
    "major_order": "col",
    "enable_rt_build": "true",
    "ignore_invalid_doc" : "true",
    "enable_recall_report": "true",
    "is_embedding_saved": "true",
    "min_scan_doc_cnt": "20000",
    "linear_build_threshold": "5000",
}
```
* dimension: the number of dimensions.
* embedding_delimiter: the vector delimiter. The default delimiter is a comma (,).
* builder_name: the index build type. We recommend that you configure the following two types:
   * LinearBuilder, offline index, when the number of data documents is less than 1w, recommend use
   * QcBuilder, quantitative clustering index
   * HnswBuilder, graph index
* searcher_name: the type of searcher that you want to use for the vector index. The value of the searcher_name parameter must match the value of the builder_name parameter. If you want to use GPU resources, contact technical support.
   * LinearSearcher, corresponding to LinearBuilder
   * QcSearcher, corresponding to QcBuilder
   * HnswSearcher, corresponding to HnswBuilder
* distance_type: the type of the distance. Valid values:
   * InnerProduct: calculates the inner product.
   * SquaredEuclidean: calculates the squared Euclidean distance. Specify SquaredEuclidean for data that is normalized.
* build_index_params: index creation parameters, which correspond to the builder_name parameter. For more information, see the build parameters of various vector indexes.
* search_index_params: the index search parameter, which corresponds to the searcher_name parameter. For more information, see the search parameter of various vector indexes.
* major_order: the method that you want to use to store your data. Valid values:
   * col: uses a column store for the data. If you set the major_order parameter to col, you must set the dimension parameter to 2 to the power of n, where n must be a positive integer. If you use the col value, the system performance is better than that when you use the row value.
   * row: uses a row store for the data. This is the default value.
* enable_rt_build: specifies whether real-time indexing is supported. Valid values: true and false.
* rt_index_params: the real-time index parameter. This parameter takes effect when the enable_rt_build is set to true. If the vector is configured with tag fields, we recommend that you set this parameter to { \"proxima.oswg.streamer.segment_size\": 2048} to prevent excessive real-time memory.
* ignore_invalid_doc: specifies whether to ignore problematic vector data. Valid values: true and false. Ignore abnormal data. If abnormal data is ignored, other fields of the abnormal data can be retrieved normally, but cannot be recalled by vector index.
* enable_recall_report: whether to report the recall indicator. true indicates reporting, false does not report, and the recall indicator is calculated by brute force retrieval after sampling query.
* linear_build_threshold: the threshold value for operations that do not use LinearBuilder. If the number of documents is less than the specified threshold value, the system uses LinearBuilder and LinearSearcher. LinearBuilder can help you reduce memory usage and ensures lossless retrieval results. The performance of LinearBuilder is decreased if an excessive number of documents exist. Default value: 10000.
* min_scan_doc_cnt: the minimum number of candidate sets that you want to retrieve. The min_scan_doc_cnt and proxima.qc.searcher.scan_ratio parameters have similar concepts. Default value: 10000. If you specify a value for the min_scan_doc_cnt parameter and specify a value for the proxima.qc.searcher.scan_ratio parameter, the larger value is used as the minimum number of candidate sets.
   * Do not specify an excessive value for the min_scan_doc_cnt or proxima.qc.searcher.scan_ratio parameter. If you specify an excessive value, the system performance is decreased and latency occurs.
   * In general, if topk vectors are recalled, the recommended size of the min_scan_doc_cnt is max(10000, 100, *topk) and scan_ratio is max(10000, 100*, topk)/total_doc_cnt. The specific parameters such as data size, recall rate, and performance must be combined.
   * These two similar parameters are used to meet the requirements in real-time and multi-category scenarios. If you are a regular user, you can configure only the proxima.qc.searcher.scan_ratio parameter.

### linear index
The linear index is a brute-force retrieval index. All data is involved in computing during queries, so the recall rate is the highest, but the performance is poor when the data volume is large. For a linear index, you must set builder_name to LinearBuilder and set searcher_name to LinearSearcher.
#### linear index build parameters
| **Parameter** | **Restriction** | **Default value** | **Description** |
| --- | --- | --- | --- |
| proxima.linear.builder.column_major_order | string | false | Specifies how to sort the features of an index when the index is being built. Valid values: false and true. false: indicates that the features of an index are sorted row by row. true: indicates that the features of an index are sorted column by column. |

### Qc index
Qc(quantization clustering) is an inverted clustering index. In each cluster, a quantizer is used to quantize the vector to reduce the precision of the vector and improve the computational efficiency. However, the recall rate is reduced. You must specify builder_name as QcBuilder and searcher_name as QcSearcher in the vector index configuration.
#### Qc index build parameters
| **Parameter** | **Restriction** | **Default value** | **Description** |
| --- | --- | --- | --- |
| proxima.qc.builder.train_sample_count | uint32 | 0 | The volume of training data. If you set the value of this parameter to 0, all data of a document is specified as training data. |
| proxima.qc.builder.thread_count | uint32 | 0 | The number of threads that can be used. If you set the value of this parameter to 0, the number of threads that can be used is equal to the number of CPU cores of OpenSearch Vector Search Edition. |
| proxima.qc.builder.centroid_count | string | Optional | The number of centroids that you want to use for clusters. Hierarchical clusters are supported. Separate levels of hierarchical clusters with asterisks (*).  |
Sample value for hierarchical clusters that include one level: 1000.
Sample value for hierarchical clusters that include two levels: 100*100.
If you want to specify the number of centroids for hierarchical clusters that include two levels, we recommend that you specify more centroids for the first level than the second level. This ensures a result that is better than the result obtained when you specify less centroids for the first level than the second level. The experience points that can be obtained in the first level are 10 times those in the second level.
If you do not specify the number of centroids, the system automatically infers the appropriate number of centroids. We recommend that you allow the system to automatically infer the number of centroids.  |
| proxima.qc.builder.cluster_auto_tuning | bool | false | Specifies whether to enable adaptive number of central points |
| proxima.qc.builder.optimizer_class | string | HcBuilder | The optimizer for the center part, which is used to improve the accuracy during classification. Subsequent queries for online candidate center parts are based on this method. We recommend that you configure this parameter to BruteForceBuilder |
| proxima.qc.builder.optimizer_params | IndexParams | - | The build and retrieval parameters corresponding to the optimize method. For example, if the optimizer is configured with Hnswbuilder, the parameters can be configured as follows:
| proxima.qc.builder.quantizer_class | string | - | Configure a quantizer. By default, no quantizer is used. Valid values: Int8QuantizerConverter, HalfFloatConverter, and DoubleBitConverter. General configuration of quantizers improves performance, reduces index size, and loses recall as appropriate |
| proxima.qc.builder.quantize_by_centroid | bool | False | Specifies whether to perform quantization based on the center point when the proxima.qc.builder.quantizer_class is used. Currently, only the case where the proxima.qc.builder.quantizer_class is an Int8 QuantizerConverter is supported |
| proxima.qc.builder.store_original_features | bool | False | Whether to retain the original feature. When using proxima.qc.builder.quantizer_class, the features obtained by IndexProvider are quantized. You need to start this option to obtain the original features |
| proxima.qc.builder.train_sample_count | uint32 | 0 | The maximum number of documents to be sampled when each segment is clustered. This reduces the number and improves the clustering speed. |
| proxima.qc.builder.train_sample_ratio | float | 1 | The proportion of documents to be clustered in each segment. The maximum value of documents with the train_sample_ratio proportion and train_sample_count is the number of documents to be clustered. |
#### Qc index search parameters
| **Parameter** | **Restriction** | **Default value** | **Description** |
| --- | --- | --- | --- |
| proxima.qc.searcher.scan_ratio | float | 0.01 | The maximum ratio of documents that can be scanned during the search to all documents. This parameter is used to calculate the value of the max_scan_num parameter. The following formula is used to calculate this value: Value of the max_scan_num parameter = Total number of documents × Value of the proxima.qc.searcher.scan_ratio parameter. |
| proxima.qc.searcher.optimizer_params | IndexParams | N/A | The online retrieval parameters that correspond to the optimizer specified when you specify the builder. For example, if you specify the optimizer as HnswBuilder in an offline build phase, you can specify the following retrieval parameter that corresponds to HnswSearcher: |
| proxima.hnsw.searcher.max_scan_ratio: 0.1 |
| proxima.qc.searcher.brute_force_threshold | int | 1000 | A threshold value. If the total number of documents is less than this threshold value, linear retrieval is performed. |

### HNSW indexes
HNSW is short for Hierarchical Navigable Small World, which is a vector index based on hierarchical small world graph retrieval algorithm. Compared with QC, HNSW does not need to adjust parameters many times to obtain better recall rate and query performance. However, the index size of HNSW is much larger than Qc, and the memory overhead is also much larger than Qc. For an HNSW index, you must set the builder_name parameter to HnswBuilder and the searcher_name parameter to HnswSearcher.

#### Index building parameters for HNSW
| **Parameter** | **Restriction** | **Default value** | **Description** |
| --- | --- | --- | --- |
| proxima.hnsw.builder.max_neighbor_count | uint32 | 100 | The maximum number of neighbors for a node in a graph. A larger value indicates a better connectivity of the graph. Correspondingly, the graph building cost and index size also increase.  |
| proxima.hnsw.builder.efconstruction | uint32 | 500 | The size of the neighboring area that can be scanned when a graph is being built. A larger value indicates a higher quality of the offline graph building but a lower index building speed. We recommend that you set the value to 400 for the first time. |
| proxima.hnsw.builder.thread_count | uint32 | 0 | The number of threads that can be used. If you set this parameter to 0, the number of threads that can be used is equal to the number of CPU cores of an OpenSearch Vector Search Edition instance. |

#### HNSW index search parameters
| **Parameter** | **Restriction** | **Default value** | **Description** |
| --- | --- | --- | --- |
| proxima.hnsw.searcher.ef | uint32 | 500 | The number of the nearest neighbors that are scanned during an online search. A larger value indicates higher recall and poorer performance. We recommend that you set this parameter to a [100,100 0] value.  |

