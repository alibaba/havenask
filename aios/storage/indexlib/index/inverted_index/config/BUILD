load('//aios/storage:defs.bzl', 'strict_cc_library')
package(default_visibility=['//aios/storage/indexlib:__subpackages__'])
strict_cc_library(
    name='config',
    srcs=[],
    hdrs=[],
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[
        ':DateIndexConfig', ':InvertedIndexConfig', ':PackageIndexConfig',
        ':RangeIndexConfig', ':SpatialIndexConfig'
    ]
)
strict_cc_library(
    name='InvertedIndexConfig',
    srcs=['InvertedIndexConfig.cpp', 'InvertedIndexConfigSerializer.cpp'],
    hdrs=['InvertedIndexConfig.h', 'InvertedIndexConfigSerializer.h'],
    deps=[
        ':AdaptiveDictionaryConfig', ':DictionaryConfig',
        ':HighFreqVocabularyCreator', ':PayloadConfig', ':TruncateIndexConfig',
        ':TruncateProfileConfig', ':TruncateTermVocabulary',
        '//aios/autil:env_util', '//aios/storage/indexlib/config:ConfigDefine',
        '//aios/storage/indexlib/config:FileCompressConfig',
        '//aios/storage/indexlib/config:IIndexConfig',
        '//aios/storage/indexlib/config:IndexConfigDeserializeResource',
        '//aios/storage/indexlib/index/ann:constants',
        '//aios/storage/indexlib/index/inverted_index:Types',
        '//aios/storage/indexlib/index/inverted_index:constants',
        '//aios/storage/indexlib/index/primary_key:constants',
        '//aios/storage/indexlib/util:status_to_exception'
    ]
)
strict_cc_library(name='SingleFieldIndexConfig', deps=[':InvertedIndexConfig'])
strict_cc_library(name='RangeIndexConfig', deps=[':SingleFieldIndexConfig'])
strict_cc_library(
    name='DateIndexConfig',
    deps=[':DateLevelFormat', ':SingleFieldIndexConfig']
)
strict_cc_library(name='SpatialIndexConfig', deps=[':SingleFieldIndexConfig'])
strict_cc_library(
    name='PackageIndexConfig',
    deps=[
        ':InvertedIndexConfig', ':SectionAttributeConfig',
        '//aios/storage/indexlib/util:key_value_map'
    ]
)
strict_cc_library(
    name='DictionaryConfig',
    deps=[
        '//aios/autil:json', '//aios/storage/indexlib/base:Status',
        '//aios/storage/indexlib/config:ConfigDefine'
    ]
)
strict_cc_library(
    name='SectionAttributeConfig',
    deps=[
        '//aios/autil:json', '//aios/storage/indexlib/index/attribute/config',
        '//aios/storage/indexlib/index/inverted_index:constants'
    ]
)
strict_cc_library(name='DateLevelFormat', deps=['//aios/autil:log'])
strict_cc_library(
    name='AdaptiveDictionaryConfig',
    deps=[
        '//aios/autil:json', '//aios/storage/indexlib/base:Status',
        '//aios/storage/indexlib/config:ConfigDefine',
        '//aios/storage/indexlib/util:Exception'
    ]
)
strict_cc_library(
    name='HighFrequencyVocabulary',
    deps=[
        '//aios/autil:log', '//aios/storage/indexlib/base:Status',
        '//aios/storage/indexlib/file_system',
        '//aios/storage/indexlib/file_system/relocatable:relocator',
        '//aios/storage/indexlib/index/common:DictHasher'
    ]
)
strict_cc_library(
    name='HighFreqVocabularyCreator',
    deps=[
        ':DictionaryConfig', ':HighFrequencyVocabulary',
        '//aios/autil:string_helper',
        '//aios/storage/indexlib/index/common:DictKeyInfo',
        '//aios/storage/indexlib/index/inverted_index:Types',
        '//aios/storage/indexlib/util:path_util'
    ]
)
strict_cc_library(
    name='DiversityConstrain', deps=['//aios/autil:json', '//aios/autil:log']
)
strict_cc_library(
    name='TruncateProfile',
    deps=[
        ':TruncateProfileConfig', '//aios/autil:json', '//aios/autil:log',
        '//aios/storage/indexlib/config:SortParam'
    ]
)
strict_cc_library(
    name='TruncateProfileConfig',
    deps=[
        ':PayloadConfig', '//aios/autil:json', '//aios/autil:log',
        '//aios/storage/indexlib/base:Status',
        '//aios/storage/indexlib/config:ConfigDefine',
        '//aios/storage/indexlib/config:SortParam'
    ]
)
strict_cc_library(
    name='TruncateStrategy',
    deps=[
        ':DiversityConstrain', '//aios/autil:json', '//aios/autil:log',
        '//aios/storage/indexlib/index/inverted_index:Types'
    ]
)
strict_cc_library(
    name='TruncateOptionConfig',
    deps=[
        ':TruncateIndexConfig', '//aios/autil:json', '//aios/autil:log',
        '//aios/storage/indexlib/config:IIndexConfig',
        '//aios/storage/indexlib/index/inverted_index/config:InvertedIndexConfig'
    ]
)
strict_cc_library(
    name='PayloadConfig', deps=['//aios/autil:hash_base', '//aios/autil:log']
)
strict_cc_library(
    name='TruncateIndexConfig',
    deps=[':TruncateIndexProperty', '//aios/autil:json', '//aios/autil:log']
)
strict_cc_library(
    name='TruncateIndexProperty',
    srcs=[],
    deps=[
        ':TruncateProfile', ':TruncateStrategy', '//aios/autil:json',
        '//aios/autil:log'
    ]
)
strict_cc_library(
    name='TruncateIndexNameMapper',
    deps=[
        '//aios/autil:json', '//aios/autil:log',
        '//aios/storage/indexlib/base:Status',
        '//aios/storage/indexlib/config:IIndexConfig',
        '//aios/storage/indexlib/file_system:interface',
        '//aios/storage/indexlib/index/inverted_index/config:InvertedIndexConfig'
    ]
)
strict_cc_library(
    name='TruncateTermVocabulary',
    deps=[
        '//aios/autil:log', '//aios/autil:string_helper',
        '//aios/storage/indexlib/base:Types',
        '//aios/storage/indexlib/file_system',
        '//aios/storage/indexlib/index/common:DictKeyInfo'
    ]
)
