load('//aios/storage:defs.bzl', 'strict_cc_fast_test')
strict_cc_fast_test(
    name='attribute_unittest',
    srcs=[
        'AttributeConvertorTest.cpp', 'CompressFloatAttributeConvertorTest.cpp',
        'CompressSingleFloatAttributeConvertorTest.cpp',
        'DateAttributeConvertorTest.cpp',
        'MultiStringAttributeConvertorTest.cpp',
        'MultiValueAttributeConvertorTest.cpp',
        'MultiValueAttributeFormatterTest.cpp',
        'SingleValueAttributeConvertorTest.cpp',
        'StringAttributeConvertorTest.cpp', 'TimeAttributeConvertorTest.cpp',
        'TimestampAttributeConvertorTest.cpp'
    ],
    copts=['-fno-access-control'],
    shard_count=5,
    deps=[
        '//aios/storage/indexlib/index/common:Types',
        '//aios/storage/indexlib/index/common/field_format/attribute:AttributeConvertorFactory',
        '//aios/storage/indexlib/index/common/field_format/attribute:CompressFloatAttributeConvertor',
        '//aios/storage/indexlib/index/common/field_format/attribute:CompressSingleFloatAttributeConvertor',
        '//aios/storage/indexlib/index/common/field_format/attribute:DateAttributeConvertor',
        '//aios/storage/indexlib/index/common/field_format/attribute:LineAttributeConvertor',
        '//aios/storage/indexlib/index/common/field_format/attribute:LocationAttributeConvertor',
        '//aios/storage/indexlib/index/common/field_format/attribute:MultiStringAttributeConvertor',
        '//aios/storage/indexlib/index/common/field_format/attribute:MultiValueAttributeConvertor',
        '//aios/storage/indexlib/index/common/field_format/attribute:MultiValueAttributeFormatter',
        '//aios/storage/indexlib/index/common/field_format/attribute:PolygonAttributeConvertor',
        '//aios/storage/indexlib/index/common/field_format/attribute:SingleValueAttributeConvertor',
        '//aios/storage/indexlib/index/common/field_format/attribute:StringAttributeConvertor',
        '//aios/storage/indexlib/index/common/field_format/attribute:TimeAttributeConvertor',
        '//aios/storage/indexlib/index/common/field_format/attribute:TimestampAttributeConvertor',
        '//aios/storage/indexlib/index/common/field_format/spatial:shape',
        '//aios/unittest_framework'
    ]
)
