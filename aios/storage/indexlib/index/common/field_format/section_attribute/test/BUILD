load('//aios/storage:defs.bzl', 'strict_cc_fast_test')
strict_cc_fast_test(
    name='SectionAttributeFormatterTest',
    srcs=['SectionAttributeFormatterTest.cpp'],
    copts=['-fno-access-control'],
    deps=[
        '//aios/autil:NoCopyable', '//aios/autil:log',
        '//aios/autil:string_type', '//aios/storage/indexlib/base:Status',
        '//aios/storage/indexlib/index/common/field_format/section_attribute:SectionAttributeFormatter',
        '//aios/unittest_framework'
    ]
)
strict_cc_fast_test(
    name='SectionAttributeEncoderTest',
    srcs=['SectionAttributeEncoderTest.cpp'],
    copts=['-fno-access-control'],
    deps=[
        '//aios/autil:NoCopyable', '//aios/autil:log',
        '//aios/storage/indexlib/base:Status',
        '//aios/storage/indexlib/index/common/field_format/section_attribute:MultiSectionMeta',
        '//aios/storage/indexlib/index/common/field_format/section_attribute:SectionAttributeEncoder',
        '//aios/unittest_framework'
    ]
)
strict_cc_fast_test(
    name='MultiSectionMetaTest',
    srcs=['MultiSectionMetaTest.cpp'],
    copts=['-fno-access-control'],
    deps=[
        '//aios/storage/indexlib/base:Types',
        '//aios/storage/indexlib/index/common/field_format/section_attribute:MultiSectionMeta',
        '//aios/unittest_framework'
    ]
)
