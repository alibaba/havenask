---
toc: content
order: 7
---


# 向量索引

向量检索是指将图片或者内容等以向量的形式表达，并建立向量索引库，查询时支持输入一个或多个向量来根据向量距离返回topK个近似结果。  
问天引擎深度集成了[proxima](https://github.com/alibaba/proxima)核心库，使问天引擎具有了向量检索的能力。问天引擎的向量索引支持图索引、聚类索引等多种主流的向量索引类型，并具有高性能、低成本的特点。以hnsw索引为例，在sift 1M和deep 10M公开测试集上，使用16core 64G的机器，问天引擎的向量索引可以达到数千qps和10ms以内的延迟。同时问天引擎的向量索引可以采用非全内存的模式加载，从而可以大大减少了对机器内存的开销，提升单机索引规模，降低机器成本。

向量检索的示例请参考[向量示例](https://github.com/alibaba/havenask/tree/main/example/cases/vector)

## 不带类目的向量配置
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
## 带有类目的向量配置
引入分类的目的是为了支持按照分类进行向量检索，比如一个图片有不同的类别，如果不指定分类构建向量索引，只是对检索出来的向量进行过滤很可能会出现无结果的情况。查询时需要指定待检索的类目，如果不指定的话引擎会查询所有类目，性能会急剧下降。
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

- findex_fields：构建向量索引的字段，必须为RAW类型，最少包括2个字段，一个是主键（或者是主键的hash值），字段值必须是整数，另一个是包含向量的字段。如果需要对向量按照分类构建索引，可以新增一个分类字段，字段类型为RAW类型，字段值为整数。这些字段在索引中的顺序必须和在fields配置的顺序相同，而且必须是按照主键、类目（如果有）、向量这个顺序。RAW类型的字段可以通过内置的DupFieldProcessor从原始字段拷贝到目标字段，不需要再数据中添加RAW类型的字段，配置方式参考[向量数据处理配置](https://github.com/alibaba/havenask/tree/main/hape/example/cases/vector/vector_conf/cluster_templates/havenask/direct_table/data_tables/default_table.json)。
- name：向量索引的名称。
- index_type：索引类型，必须为ANN。
- indexer：构建向量索引的插件，目前仅支持aitheta2_indexer。
- parameters：向量索引的构建参数和查询参数，不同索引类型参数不同


## 向量索引详细配置
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
* dimension：维度
* embedding_delimiter: 向量分隔符，默认是","
* builder_name: 索引构建类型，建议配置下面两种
  * LinearBuilder，线下索引，数据文档数量少于1w时，推荐使用
  * QcBuilder，量化聚类索引
  * HnswBuilder，图索引
* searcher_name: 索引检索类型，和builder_name对应，如果有GPU需求请联系我们。
  * LinearSearcher，对应LinearBuilder
  * QcSearcher，对应QcBuilder
  * HnswSearcher，对应HnswBuilder
* distance_type：距离类型，目前仅支持如下两种
  * InnerProduct （内积）
  * SquaredEuclidean（平方欧式距离），经过归一化的数据请配置SquaredEuclidean。
* build_index_params：索引构建参数，和builder_name参数对应，具体见各种向量索引的build参数。
* search_index_params：索引检索参数，和searcher_name参数对应，具体见各种向量索引的search参数。
* major_order: 数据存储方式，目前支持如下两种
  * col （按列存, 对dimension有要求，必须是2的幂次方, 性能更优）
  * row（按行存，默认使用）
* enable_rt_build：是否支持实时索引，true支持，false不支持。
* rt_index_params：实时索引参数，当enable_rt_build为true时生效，当向量配置了标签字段时建议配置：{  \"proxima.oswg.streamer.segment_size\": 2048}，防止实时内存过大。
* ignore_invalid_doc：是否忽略有问题的向量数据，true忽略异常数据，false不忽略。忽略异常数据时，异常数据的其他字段可以正常检索，只是无法通过向量索引进行召回。
* enable_recall_report：是否汇报recall指标，true表示汇报，false不汇报，recall指标是通过对query进行采样之后暴力检索计算得出。
* linear_build_threshold：线性构建的阈值，若文档数量低于该阈值，则会使用LinearBuilder构建, LinearSearcher检索。默认是10000，用线性构建的好处是可以节省内存,召回结果无损, 但是若数据规模较大时，性能极差。
* min_scan_doc_cnt: 召回候选集的个数最小值，默认10000，和proxima.qc.searcher.scan_ratio的概念类似。两者都配置的情况下，取两者的最大值
  * scan_ratio&min_scan_doc_cnt不是越大越好。太大的话，会极大影响性能&延迟
  * 一般而言, 若召回topk个向量，min_scan_doc_cnt的建议大小为max(10000, 100*topk)，scan_ratio为max(10000, 100*topk)/total_doc_cnt, 具体的还得结合数据规模、召回率以及性能等参数。
  * 之所以存在两个类似参数，主要是由于实时以及多类目场景下的需求。一般用户只需要配置scan_ratio即可

### linear索引
linear索引是一种暴力检索的索引，查询时所有的数据都会参与计算，所以召回率最高，但是当数据量大时性能较差。linear索引需要在向量索引配置中将builder_name指定为LinearBuilder，searcher_name指定为LinearSearcher。
#### linear索引build参数
| **参数名** | **类型** | **默认值** | **说明** |
| --- | --- | --- | --- |
| proxima.linear.builder.column_major_order | string | false | 构建的时候特征用行排（false）/列排（true） |

### Qc索引
Qc（quantization clustering）是一种倒排聚类型索引，在每个聚类下使用量化器对向量进行量化降低向量的精度，提升计算效率，但是对召回率有有一定的损失。Qc索引需要在向量索引配置中将builder_name指定为QcBuilder，searcher_name指定为QcSearcher。
#### Qc索引build参数
| **参数名** | **类型** | **默认值** | **说明** |
| --- | --- | --- | --- |
| proxima.qc.builder.train_sample_count | uint32 | 0 | 指定训练数据量，如果为0则使用全部数据 |
| proxima.qc.builder.thread_count | uint32 | 0 | 构建时开启线程数量，设置为0时为cpu核数 |
| proxima.qc.builder.centroid_count | string | 可选 | 聚类中心点参数，支持层次聚类。层之间用“*”分隔。
一层聚类示例：1000
两层示例：100*100
如果使用两层中心点，一般第一次中心点数量比第二层多，效果更好。经验值是第一层是第二层10倍。
未配置时，系统会自动推导出合适的中心点个数，建议由系统自动推导。 |
| proxima.qc.builder.cluster_auto_tuning | bool | false | 指定是否开启中心点数目自适应 |
| proxima.qc.builder.optimizer_class | string | HcBuilder | 针对中心点部分的优化器，用于提升分类时的精度，后续在线候选中心点部分的查询均基于此方法进行，建议配置为BruteForceBuilder |
| proxima.qc.builder.optimizer_params | IndexParams | - | optimize方法对应的构建和检索参数，比如optimizer配置了Hnswbuilder，那么该处参数可配置为：
| proxima.qc.builder.quantizer_class | string | - | 配置量化器，默认不使用量化器。可选有 Int8QuantizerConverter, HalfFloatConverter, DoubleBitConverter。一般配置量化器可提升性能，减少索引大小，召回视情况有所损失 |
| proxima.qc.builder.quantize_by_centroid | bool | False | 使用proxima.qc.builder.quantizer_class时，是否按中心点进行量化。目前仅支持 proxima.qc.builder.quantizer_class 为 Int8QuantizerConverter 的情况 |
| proxima.qc.builder.store_original_features | bool | False | 是否保留原始特征。使用proxima.qc.builder.quantizer_class 时，IndexProvider 获取的特征是量化后的，需要开始此选项，才能获取原始特征 |
| proxima.qc.builder.train_sample_count | uint32 | 0 | 每个segment聚类时抽样的最大文档个数，减少提高聚类速度|
| proxima.qc.builder.train_sample_ratio | float | 1 | 每个segment聚类文档的比例，取train_sample_ratio比例的文档与train_sample_count取最大值为聚类的文档数|
#### Qc索引search参数
| **参数名** | **类型** | **默认值** | **说明** |
| --- | --- | --- | --- |
| proxima.qc.searcher.scan_ratio | float | 0.01 | 用于计算max_scan_num数量，总doc数量 * scan_ratio |
| proxima.qc.searcher.optimizer_params | IndexParams | 空 | 指定Build时指定的optimizer对应的在线检索参数，比如离线构建时的optimizer指定为HnswBuilder，那么此处可指定HnswSearcher对应的检索参数：
proxima.hnsw.searcher.max_scan_ratio: 0.1 |
| proxima.qc.searcher.brute_force_threshold | int | 1000 | 如果总doc数少于此值，则走线性检索 |

### HNSW索引
HNSW是Hierarchical Navigable Small World的简称，是一种基于分层小世界图检索算法的向量索引。HNSW与QC相比，不需要经过多次调参即可获得比较好的召回率和查询性能。但是HNSW的索引大小要远大于Qc，内存的开销也远大于Qc。HNSW索引需要在向量索引配置中将builder_name指定为HnswBuilder，searcher_name指定为HnswSearcher。

#### HNSW索引build参数
| **参数名** | **类型** | **默认值** | **说明** |
| --- | --- | --- | --- |
| proxima.hnsw.builder.max_neighbor_count | uint32 | 100 | 指定图中节点最大邻居数。该值越大，代表图的连通性越好，相应的构图成本和索引size也会增加。 |
| proxima.hnsw.builder.efconstruction | uint32 | 500 | 指控制图构建过程中近邻扫描区域大小，该值越大，离线构图质量越好，索引构建越慢。建议初始从400配置|
| proxima.hnsw.builder.thread_count | uint32 | 0 | 构建时开启线程数量，设置为0时为cpu核数 |

#### HNSW索引search参数
| **参数名** | **类型** | **默认值** | **说明** |
| --- | --- | --- | --- |
| proxima.hnsw.searcher.ef | uint32 | 500 | 用于控制在线检索时考察的子图范围大小。该值越大，召回越高，性能越差，建议取值[100,1000]。 |

