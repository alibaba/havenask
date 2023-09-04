load('//aios/storage:defs.bzl', 'strict_cc_fast_test')
strict_cc_fast_test(
    name='SpatialIndexReaderTest',
    srcs=['SpatialIndexReaderTest.cpp'],
    copts=['-fno-access-control'],
    deps=[
        '//aios/storage/indexlib/index/attribute:indexer',
        '//aios/storage/indexlib/index/common/field_format/spatial:shape',
        '//aios/storage/indexlib/index/inverted_index/builtin_index/spatial:SpatialIndexReader',
        '//aios/storage/indexlib/index/inverted_index/builtin_index/spatial:SpatialPostingIterator',
        '//aios/unittest_framework'
    ]
)
