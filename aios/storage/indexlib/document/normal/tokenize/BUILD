load('//aios/storage:defs.bzl', 'strict_cc_library')
package(default_visibility=['//aios/storage/indexlib:__subpackages__'])
strict_cc_library(
    name='tokenize',
    srcs=[],
    hdrs=[],
    deps=[
        ':AnalyzerToken', ':TokenizeDocument', ':TokenizeField',
        ':TokenizeSection'
    ]
)
strict_cc_library(
    name='AnalyzerToken',
    deps=[
        '//aios/autil:data_buffer', '//aios/autil:hash_base',
        '//aios/storage/indexlib/index/inverted_index:Types'
    ]
)
strict_cc_library(
    name='TokenizeSection',
    deps=[
        ':AnalyzerToken', '//aios/autil:lock', '//aios/autil:log',
        '//aios/autil:object_allocator',
        '//aios/storage/indexlib/index/inverted_index:Types'
    ]
)
strict_cc_library(
    name='TokenizeField',
    deps=[
        ':TokenizeSection', '//aios/autil:log',
        '//aios/storage/indexlib/base:Types'
    ]
)
strict_cc_library(name='TokenizeDocument', deps=[':TokenizeField'])
