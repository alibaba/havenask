load('//aios/storage:defs.bzl', 'strict_cc_fast_test')
strict_cc_fast_test(
    name='base_unittest',
    srcs=[
        'BinaryStringUtilTest.cpp', 'ConstantTest.cpp',
        'MemoryQuotaControllerTest.cpp', 'NoExceptionWrapperTest.cpp',
        'PathUtilTest.cpp', 'StatusTest.cpp'
    ],
    data=[],
    deps=[
        '//aios/autil:multi_value', '//aios/autil:thread',
        '//aios/storage/indexlib/base:BinaryStringUtil',
        '//aios/storage/indexlib/base:MemoryQuotaController',
        '//aios/storage/indexlib/base:NoExceptionWrapper',
        '//aios/storage/indexlib/base:PathUtil',
        '//aios/storage/indexlib/base:Status',
        '//aios/storage/indexlib/util:Exception',
        '//aios/storage/indexlib/util/testutil:unittest'
    ]
)
