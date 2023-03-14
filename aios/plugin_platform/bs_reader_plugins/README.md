# 在线配置需要打开实时
```
realtime": true
```
参考:`example/cases/normal/config/online_config/table/0/clusters/in0_cluster.json`

# realtime_info.json参数
参考:`example/cases/normal/config/realtime_info.json`
```
{
    "realtime_mode":"realtime_service_rawdoc_rt_build_mode",
    "data_table" : "in0",
    "type":"plugin",
    "module_name":"kafka",
    "module_path":"libbs_raw_doc_reader_plugin.so",
    "topic_name":"quickstart-events",
    "bootstrap.servers":"localhost:9092",
    "src_signature":"16113477091138423397",
    "realtime_process_rawdoc": "true"
}
```
参数含义
- realtime_mode: 实时模式
- data_table: 表名
- type": 数据源类型
- module_name": 插件模块名称
- module_path: 插件加载路径
- topic_name: 实时数据对应的kafka topic名字
- bootstrap.servers: kafka的bootstrap配置
- src_signature: 数据源签名
- realtime_process_rawdoc: 是否需要process原始文档
可选参数:
- kafka_start_timestamp: 起始消费时间戳(us)，默认从topic的起始开始消费

# 主键Hash函数
- java版本的HASH函数实现代码: `aios/plugin_platform/bs_reader_plugins/util/HashAlgorithm.java`
实时消费消息时，会按照消息key的hash值做range做过滤，所以producer的partitioner要使用相同的hash函数和划分partition的策略
```
hashId = getHashId(key);
partId = getPartitionId(hashId, kafkaPartitionCnt);
```

# Note
实时索引只会build比全量和增量更新的消息