load('//aios/storage:defs.bzl', 'strict_cc_fast_test')
package(default_visibility=['//visibility:private'])
strict_cc_fast_test(
    name='AttributeConfigTest',
    srcs=['AttributeConfigTest.cpp'],
    copts=['-fno-access-control'],
    deps=[
        '//aios/storage/indexlib/config:schema',
        '//aios/storage/indexlib/framework:tablet_schema_loader',
        '//aios/storage/indexlib/framework/mock:factory',
        '//aios/storage/indexlib/index/attribute:factory',
        '//aios/storage/indexlib/index/attribute/config',
        '//aios/unittest_framework'
    ]
)
