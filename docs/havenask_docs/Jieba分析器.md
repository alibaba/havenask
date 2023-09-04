# 分析器介绍
* Jieba分析器（jieba_analyzer）基于开源的jieba分词器实现
```
原始内容：他来到了网易杭研大厦
jieba分析器：他/来到/了/网易/杭研/大厦
```

* 例子可参考，[自定义分析器示例](../../hape/example/cases/custom_analyzer)

* 具体实现可参考，[jieba分析器](../../aios/plugins/havenask_plugins/analyzer_plugins)

# 注意事项
- 该分析器只适用于TEXT类型字段