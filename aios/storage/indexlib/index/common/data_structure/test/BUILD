load('//aios/storage:defs.bzl', 'strict_cc_fast_test')
strict_cc_fast_test(
    name='datastructure_unittest',
    srcs=[
        'AdaptiveAttributeOffsetDumperTest.cpp',
        'AttributeCompressInfoTest.cpp', 'EqualValueCompressDumperTest.cpp',
        'ExpandableValueAccessorTest.cpp', 'VarLenDataParamHelperTest.cpp',
        'VarLenDataTest.cpp'
    ],
    copts=['-fno-access-control'],
    deps=[
        '//aios/storage/indexlib/file_system/test:test-util',
        '//aios/storage/indexlib/framework/mock:FakeSegment',
        '//aios/storage/indexlib/index/attribute/test:AttributeTestUtil',
        '//aios/storage/indexlib/index/common/data_structure:ExpandableValueAccessor',
        '//aios/storage/indexlib/index/common/data_structure:VarLenDataAccessor',
        '//aios/storage/indexlib/index/common/data_structure:VarLenDataDumper',
        '//aios/storage/indexlib/index/common/data_structure:VarLenDataMerger',
        '//aios/storage/indexlib/index/common/data_structure:VarLenDataParamHelper',
        '//aios/storage/indexlib/index/test:FakeDocMapper',
        '//aios/storage/indexlib/util/testutil:unittest',
        '//aios/unittest_framework'
    ]
)
