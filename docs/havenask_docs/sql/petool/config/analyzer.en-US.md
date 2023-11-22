---
toc: content
order: 3
---

# Analyzer configuration
## Structure
The configuration of the word divider includes three parts: analyzer configuration, tokenizer configuration and complex conversion table configuration. A complete word divider configuration file Analyser.json is as follows:
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
This section describes how to configure the word segmentation information that can be used in index construction and query. key indicates the name of the word segmentation, and value indicates the configuration information of the word segmentation. The following information can be configured for the word divider:
- tokenizer_configs: indicates the configuration information of the tokenizer used by the word classifier.
- tokenizer_type: type of the built-in tokenizer. If you need other parameters, set them to tokenizer_configs, for example, delimiter parameter for simple delimiter.
- tokenizer_name: If the tokenizer used by the classifier is not a built-in tokenizer, set the tokenizer configuration information to tokenizer_config and reference it using the tokenizer_name parameter. Do not use tokenizer_configs and tokenizer_name at the same time.
- stopwords: Stops the word list. It can be any string or empty.
- case_sensitive: indicates whether the case is sensitive. If the case is sensitive, the original text is retained. If the case is not sensitive, the original text is converted to lowercase.
- traditional_sensitive: indicates whether the simplified and traditional text is sensitive. If it is sensitive, the original text is retained; otherwise, the simplified text is changed.
- width_sensitive: indicates whether the half corner is sensitive. If the half corner is sensitive, the original text is retained. Otherwise, the half corner is changed.

### tokenizer_config
Configure the extended word segmentation information, mainly specify the dynamic library of the word segmentation and the parameters of the word segmentation.
- modules, the dynamic library information of the word divider, if both are built-in analyzer modules can be empty (i.e. []).
-- module_name: specifies the dynamic library name inside the engine, starting with a letter or an underscore.
-- module_path, dynamic library name in the plugins directory.
-- parameters, which opens the dynamic library and is passed when creating the factory corresponding to the analyzer.
- tokenizers, tokenizer configuration information. If the tokenizers are built-in analyzers, the Tokenizers can be empty (that is, []).
-- tokenizer_name: name of tokenizer, consistent with tokenizer_name in analyzer.
-- tokenizer_type indicates the tokenizer type. The dynamic library will create a corresponding tokenizer based on the tokenizer_type, so it must be consistent with the tokenizer in the code.
-- module_name, a module configured in modules, used to specify which dynamic library to use.
-- parameters, which is required for creating a tokenizer.
### traditional_tables
This parameter is optional. If there is no simplified translation table, do not configure it.

If traditional_sensitive is set to false, all traditional characters will be converted to simplified characters by default. You can set traditional_table corresponding to table_name to prevent special traditional characters from being converted to simplified characters.
- transform_table: converts information about traditional and simplified Chinese characters.

## Common analyzer introduction
### jieba Analyzer
* Jieba Analyzer (jieba_analyzer) is based on the open-source jieba word segmentation implementation
```
Original content: He came to NetEase Hangyan Building
jieba Analyzer: He/came/came/netyi/Hangyan/mansion
```

* examples for reference, and [example] custom parsers (https://github.com/alibaba/havenask/tree/main/hape/example/cases/custom_analyzer)

* specific implementation can reference, jieba analyzer (https://github.com/alibaba/havenask/tree/main/aios/plugins/havenask_plugins/analyzer_plugins)
* Precautions
- This analyzer only works with TEXT type fields

### Simple analyzer
simple analyzer uses Spaces to separate fields (or query terms). It is suitable for search scenarios that cannot be solved by the system in special scenarios, and can achieve full user control.

* Precautions
- The parser applies only to TEXT fields. Specify the parser as simple when configuring the schema.
- The analyzer does not support word segmentation.

### Word Analyzer
Word Analyzer (chn_single) classifiers words by word/word, suitable for non-semantic Chinese search scenarios.
```
Original content: Chrysanthemum Tea 123
Word Analyzer: Juhua Tea 123
```

* Precautions
- The parser applies only to TEXT fields. Specify the parser as chn_single when configuring the schema.
- The analyzer does not support word segmentation.
 