---
toc: content
order: 3
---

# 分析器配置
## 结构
分词器的配置包括analyzer配置、tokenizer配置以及繁简转换表配置三个部分，一个完整的分词器配置文件analyzer.json如下所示：
```
{   
    "analyzers":
    {
        "simple_analyzer":
        {
            "tokenizer_configs" :
            {
                "tokenizer_type" : "simple",
                "delimiter" : " "
            },
            "stopwords" : [],
            "normalize_options" :
            {
                "case_sensitive" : false,
                "traditional_sensitive" : true,
                "width_sensitive" : false
            }
        },
        "singlews_analyzer":
        {
            "tokenizer_configs" :
            {
                "tokenizer_type" : "singlews"
            },
            "stopwords" : [],
            "normalize_options" :
            {
                "case_sensitive" : false,
                "traditional_sensitive" : true,
                "width_sensitive" : false
            }
        },
        "jieba_analyzer":
        {
            "tokenizer_name" : "jieba_analyzer",
            "stopwords" : [],
            "normalize_options" :
            {
                "case_sensitive" : false,
                "traditional_sensitive" : true,
                "width_sensitive" : false
            }
        }
    },
    "tokenizer_config" : {
        "modules" : [
            {
                "module_name": "analyzer_plugin",
                "module_path": "libjieba_analyzer.so",
                "parameters": { }
            }
        ],
        "tokenizers" : [
            {
                "tokenizer_name": "jieba_analyzer",
                "tokenizer_type": "jieba",
                "module_name": "analyzer_plugin",
                "parameters": {
                    "dict_path": "/ha3_install/usr/local/share/jieba_dict/jieba.dict.utf8",
                    "hmm_path": "/ha3_install/usr/local/share/jieba_dict/hmm_model.utf8",
                    "user_dict_path": "/ha3_install/usr/local/share/jieba_dict/user.dict.utf8",
                    "idf_path": "/ha3_install/usr/local/share/jieba_dict/idf.utf8",
                    "stop_word_path": "/ha3_install/usr/local/share/jieba_dict/stop_words.utf8"
                }
            }
        ]
    },
    "traditional_tables": [
        {
            "table_name" : "table_name",
            "transform_table" : {
                "0x4E7E" : "0x4E7E"                                #设置繁体字"乾"不会被转换成简体字
            }
        }
    ]
}
```
### analyzers
配置可在构建索引和查询中使用的分词器信息，key为分词器的名称，value为分词器的具体配置信息。分词器可以配置如下信息：
- tokenizer_configs，分词器使用的tokenizer的配置信息。
  -  tokenizer_type，内置tokenizer的类型，如果需要其他参数也配置在tokenizer_configs中，比如简单分词的delimiter参数。
- tokenizer_name，如果分词器使用的tokenizer不是内置的需要将tokenizer配置信息配置在tokenizer_config，并通过tokenizer_name参数进行引用。tokenizer_configs和tokenizer_name不要同时使用。
- stopwords，停用词表，可以是任何字符串，也可以为空。
- case_sensitive，表示大小写是否敏感，如果敏感，则保留原始文本，否则全转成小写。
- traditional_sensitive，表示简繁体是否敏感，如果敏感，则保留原始文本，否则全转成简体。
- width_sensitive，表示全半角是否敏感，如果敏感，则保留原始文本，否则全转成半角。

### tokenizer_config
配置扩展的分词器信息，主要指定分词器的动态库和分词器的参数。
- modules，分词器的动态库信息，如果都是内置分析器modules可以为空（即[]）。
  -- module_name，动态库在引擎内部的名称，字母或者下划线开头。
  -- module_path，plugins目录下的动态库名称。
  -- parameters，打开动态库，创建对应分析器的factory时需要传递的参数。
- tokenizers，tokenizer具体配置信息，如果为内置分析器tokenizers可以为空（即[]）。
  -- tokenizer_name，tokenizer名称，与analyzer中的tokenizer_name一致。
  -- tokenizer_type，tokenizer类型，动态库会根据tokenizer_type创建对应的tokenizer，所以需要与代码中的tokenizer一致。
  -- module_name，modules中配置的某一个module，用与指定使用哪个动态库。
  -- parameters，创建tokenizer所需要的参数。
### traditional_tables
配置繁简体转化表，可选参数，如果没有可以不配置。

- table_name，表示简繁体转换表，当traditional_sensitive为false时，默认会全转成简体，可以通过设置table_name对应的traditional_table使得特殊的繁体字不会被转成简体字。
- transform_table，繁体字与简体字转换信息。

## 常用分析器介绍
### jieba分析器
* Jieba分析器（jieba_analyzer）基于开源的jieba分词器实现
```
原始内容：他来到了网易杭研大厦
jieba分析器：他/来到/了/网易/杭研/大厦
```

* 例子可参考，[自定义分析器示例](https://github.com/alibaba/havenask/tree/main/hape/example/cases/custom_analyzer)

* 具体实现可参考，[jieba分析器](https://github.com/alibaba/havenask/tree/main/aios/plugins/havenask_plugins/analyzer_plugins)
* 注意事项
  - 该分析器只适用于TEXT类型字段

### 简单分析器
简单分析器（simple）使用空格“ ”对字段内容（或查询词）进行分隔，适合特殊场景下系统自带无法解决的搜索场景，可以实现完全用户控制的效果。

* 注意事项
  - 该分析器只适用于TEXT类型字段，在配置schema的时候指定分析器为simple。
  - 该分析器不支持分词干预。

### 单字分析器
单字分析器（chn_single）按照单字/单词分词，适合非语义的中文搜索场景。
```
原始内容：菊花茶123
单字分析器：菊 花 茶 123
```

* 注意事项
  - 该分析器只适用于TEXT类型字段，在配置schema的时候指定分析器为chn_single。
  - 该分析器不支持分词干预。