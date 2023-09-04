## 描述
为便于定制各种不同的应用，在查询语法中引入kvpair，即key-value对。

## 语法格式
`kvpair={key}:{value};{key}:{value};....`

## 示例
`query=SELECT brand, COUNT(*) FROM phone &&kvpair=trace:INFO;formatType:full_json`


| 参数名  | 取值范围 | 默认值 | 参数说明 | 
| ----------- | ----------- |----------- |----------- |
|trace | DISABLE, FATAL,ERROR,WARN,INFO,DEBUG, TRACE1, TRACE2, TRACE3, SCHEDULE, NOTSET | DISABLE | 在前端输出查询过程中的相关信息|
|formatType/format | string, json，full_json, flatbuffers | string | 返回结果类型，详情, json建议使用full_json格式，速度快建议为flatbuffers|
|timeout | ulimit | 由sql配置决定 | query查询超时限制，单位ms|
|searchInfo | true, false | false | 是否返回search info信息|
|sqlPlan | true, false | false | 是否返回sql plan信息|
|forbitMergeSearchInfo | true, false | false | qrs不合并各列search返回的searchInfo, 用于查各列的详细信息|
|resultReadable | true, false | false | 在format为json/full_json时，json中会加一些换行增加可读性|
|databaseName |   |   | 指定默认dbName，用于qrs访问对应的search，qrs也可以同时访问多个search，需要在query子句的table前拼上dbName.tableName|
|lackResultEnable | true, false | false | 允许结果缺列，例如一列rpc超时|
|iquan.optimizer.debug.enable | true/false | false | 是否开启优化阶段的debug功能|
|iquan.optimizer.sort.limit.use.together | true/false | true | 是否强制要求order by后一定要有limit|
|iquan.optimizer.force.limit.num | ulimit | 100 | 如果开启了iquan.optimizer.force.limit.enable， iquan将这个选项的值作为limit的大小|
|iquan.optimizer.join.condition.check | true/false | true | 是否强制要求join的字段为hash字段|
|iquan.optimizer.force.hash.join | true/false | false | 是否强制让所有的join节点都变为hash join|
|iquan.plan.format.type | json | json | iquan产出的执行计划的格式。目前只支持json。|
|iquan.plan.prepare.level | rel.post.optimizejni.post.optimize | jni.post.optimize | 和cache或者动态参数配合使用。推荐jni.post.optimize.当用户开启了|cache， iquan会将指定阶段的结果放入缓存中；当用户开启了动态参数， iquan会对指定阶段的结果进行动态参数替换。rel.post.optimize: 优化后的结果，在Java代码中|jni.post.optimize: JNI调用返回后的结果，在Cpp代码中|
|iquan.plan.cache.enable | true / false | false | 是否将当前的结果放入cache中。|
|exec.source.id |   | "" | 指定串访问特定行，多exchange时有用，默认按时间生成|
|exec.source.spec |   | "" | 指定业务方来源，请按照指定格式填写TPP推荐场景: tpp-appid-abid-solutionid-ip其他场景: 产品名-系统名-IP|
|dynamic_params | 二维数组 | 无 | 参考|
|urlencode_data | true / false | false | 如果对dynamic_params的内容做了urlencode，需要设置为true|


## 注意
1. kvpair和配置同时存在的项，以kvpair优先；
2. 多个kvpair之间用 ; 隔开，中间不允许有空格
