load('//aios/storage/indexlib/index/ann/aitheta2:defs.bzl', 'aitheta_cc_test')
aitheta_cc_test(
    name='params_initializer_test',
    srcs=[
        'HnswParamsInitializerTest.cpp', 'ParamsInitializerFactoryTest.cpp',
        'ParamsInitializerTest.cpp', 'QcParamsInitializerTest.cpp'
    ],
    data=[],
    deps=[
        '//aios/storage/indexlib/file_system',
        '//aios/storage/indexlib/index/ann/aitheta2/impl:segment_base',
        '//aios/storage/indexlib/index/ann/aitheta2/util/params_initializer:params_initializer_factory',
        '//aios/unittest_framework', '@proxima2//:proxima2_headers'
    ]
)
