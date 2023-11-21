load('//aios/storage:defs.bzl', 'strict_cc_fast_test')
package(default_visibility=['//visibility:private'])
strict_cc_fast_test(
    name='FieldMetaConfigTest',
    srcs=['FieldMetaConfigTest.cpp'],
    copts=['-fno-access-control'],
    deps=[
        '//aios/storage/indexlib/index/field_meta/config:FieldMetaConfig',
        '//aios/unittest_framework'
    ]
)
