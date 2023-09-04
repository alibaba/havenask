load('//aios/storage:defs.bzl', 'strict_cc_library')
strict_cc_library(
    name='tablet',
    srcs=[],
    hdrs=[],
    visibility=['//visibility:public'],
    deps=['//aios/storage/indexlib/framework:tablet']
)
strict_cc_library(
    name='document',
    srcs=[],
    hdrs=[],
    visibility=['//visibility:public'],
    deps=['//aios/storage/indexlib/document']
)
strict_cc_library(
    name='alter_table',
    srcs=[],
    hdrs=[],
    visibility=['//visibility:public'],
    deps=[
        ':document', '//aios/storage/indexlib/config:schema',
        '//aios/storage/indexlib/document/normal:NormalDocumentParser',
        '//aios/storage/indexlib/framework/index_task:IndexTaskResourceManager',
        '//aios/storage/indexlib/index/attribute:indexer'
    ]
)
strict_cc_library(
    name='interface',
    srcs=[],
    hdrs=[],
    visibility=['//visibility:public'],
    deps=[':document', ':tablet']
)
filegroup(
    name='test_log_conf',
    srcs=['test_alog.conf'],
    visibility=['//aios/storage/indexlib:__subpackages__']
)
filegroup(
    name='testdata',
    srcs=([':test_log_conf'] + glob(['testdata/**'])),
    visibility=['//aios/storage/indexlib:__subpackages__']
)
