load('//aios/storage:defs.bzl', 'strict_cc_library')
strict_cc_library(
    name='params_initializer',
    srcs=[],
    hdrs=['ParamsInitializer.h'],
    visibility=['//visibility:public'],
    deps=[
        '//aios/autil:log',
        '//aios/storage/indexlib/index/ann/aitheta2:aitheta2_index_common',
        '@proxima2//:proxima2_headers'
    ]
)
strict_cc_library(
    name='params_initializer_factory',
    srcs=['ParamsInitializerFactory.cpp'],
    hdrs=['ParamsInitializerFactory.h'],
    visibility=['//visibility:public'],
    deps=[
        ':params_initializer', ':params_initializer_impl', '//aios/autil:log',
        '//aios/storage/indexlib/index/ann/aitheta2:aitheta2_index_common',
        '@proxima2//:proxima2_headers'
    ]
)
strict_cc_library(
    name='params_initializer_impl',
    srcs=[
        'HnswParamsInitializer.cpp', 'ParamsInitializer.cpp',
        'QcParamsInitializer.cpp'
    ],
    hdrs=['HnswParamsInitializer.h', 'QcParamsInitializer.h'],
    deps=[
        ':params_initializer', '//aios/autil:log',
        '//aios/storage/indexlib/file_system',
        '//aios/storage/indexlib/index/ann/aitheta2:aitheta2_index_common',
        '@proxima2//:proxima2_headers'
    ]
)
