load('//aios/storage:defs.bzl', 'strict_cc_fast_test')
strict_cc_fast_test(
    name='IndexReclaimerTest',
    srcs=['IndexReclaimerTest.cpp'],
    copts=['-fno-access-control'],
    deps=[
        '//aios/storage/indexlib/index/inverted_index:InvertedIndexFactory',
        '//aios/storage/indexlib/table/normal_table/index_task/document_reclaim:AndIndexReclaimer',
        '//aios/storage/indexlib/table/normal_table/index_task/document_reclaim:IndexFieldReclaimer',
        '//aios/unittest_framework'
    ]
)
