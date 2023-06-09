load('//aios/storage/indexlib:defs.bzl', 'indexlib_cc_library')
package(default_visibility=[':__subpackages__'])
indexlib_cc_library(
    name='analyzer',
    srcs=[],
    hdrs=[],
    visibility=['//visibility:public'],
    deps=[':Analyzer', ':IAnalyzerFactory']
)
indexlib_cc_library(name='IAnalyzerFactory', srcs=[])
indexlib_cc_library(
    name='Analyzer',
    deps=[
        ':AnalyzerDefine', ':AnalyzerInfo', ':ITokenizer', ':SimpleTokenizer',
        ':StopWordFilter', ':TextBuffer', ':TraditionalTables',
        '//aios/autil:string_helper'
    ]
)
indexlib_cc_library(
    name='TextBuffer', deps=['//aios/autil:codec', '//aios/autil:log']
)
indexlib_cc_library(name='StopWordFilter', deps=['//aios/autil:log'])
indexlib_cc_library(
    name='AnalyzerInfo',
    deps=[
        ':AnalyzerDefine', '//aios/autil:codec', '//aios/autil:json',
        '//aios/autil:log'
    ]
)
indexlib_cc_library(name='AnalyzerDefine', srcs=[])
indexlib_cc_library(
    name='TraditionalTables', deps=['//aios/autil:json', '//aios/autil:log']
)
indexlib_cc_library(
    name='ITokenizer',
    srcs=[],
    deps=[
        '//aios/autil:json', '//aios/autil:log',
        '//aios/storage/indexlib/document/normal/tokenize:AnalyzerToken'
    ]
)
indexlib_cc_library(
    name='SimpleTokenizer', deps=[':AnalyzerDefine', ':ITokenizer']
)
