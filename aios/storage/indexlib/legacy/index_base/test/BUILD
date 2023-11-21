load('//aios/storage:defs.bzl', 'strict_cc_fast_test')
package(default_visibility=['//aios/storage/indexlib:__subpackages__'])
strict_cc_fast_test(
    name='SchemaAdapterTest',
    srcs=['SchemaAdapterTest.cpp'],
    copts=['-fno-access-control'],
    deps=[
        '//aios/storage/indexlib/base:constants',
        '//aios/storage/indexlib/file_system/test:test-util',
        '//aios/storage/indexlib/legacy/config/test:schema_maker',
        '//aios/storage/indexlib/legacy/index_base:SchemaAdapter',
        '//aios/storage/indexlib/util/testutil:unittest'
    ]
)
