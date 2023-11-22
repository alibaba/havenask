load('//aios/storage:defs.bzl', 'strict_cc_fast_test')
strict_cc_fast_test(
    name='FieldMetaDiskIndexerTest',
    srcs=['FieldMetaDiskIndexerTest.cpp'],
    copts=['-fno-access-control'],
    deps=[
        '//aios/storage/indexlib/index/field_meta:FieldMetaDiskIndexer',
        '//aios/storage/indexlib/index/field_meta:FieldMetaMemIndexer',
        '//aios/unittest_framework'
    ]
)
