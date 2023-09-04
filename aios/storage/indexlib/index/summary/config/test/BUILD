load('//aios/storage:defs.bzl', 'strict_cc_fast_test')
strict_cc_fast_test(
    name='summary_index_config_test',
    srcs=['SummaryIndexConfigTest.cpp'],
    copts=['-fno-access-control'],
    deps=[
        '//aios/storage/indexlib/config:MutableJson',
        '//aios/storage/indexlib/config:SingleFileCompressConfig',
        '//aios/storage/indexlib/index/summary/config:SummaryIndexConfig',
        '//aios/unittest_framework'
    ]
)
