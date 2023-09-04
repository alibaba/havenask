package(default_visibility=['//aios/sql:__subpackages__'])
cc_library(
    name='sql_log',
    hdrs=glob(['Log.h']),
    include_prefix='sql/common',
    visibility=['//visibility:public'],
    deps=['//aios/autil:log', '//aios/navi']
)
cc_library(
    name='sql_common_h',
    srcs=glob(['common.cpp']),
    hdrs=glob(['common.h']),
    include_prefix='sql/common',
    deps=[]
)
cc_library(
    name='sql_tracer',
    srcs=glob(['TracerAdapter.cpp']),
    hdrs=glob(['TracerAdapter.h']),
    include_prefix='sql/common',
    deps=[':sql_common_h', ':sql_log', '//aios/turing_ops_util:trace']
)
cc_library(
    name='sql_auth',
    srcs=['SqlAuthManagerR.cpp'],
    hdrs=['SqlAuthManagerR.h'],
    include_prefix='sql/common',
    deps=[
        ':sql_common_h', ':sql_log', '//aios/autil:hash_func',
        '//aios/sql/config:sql_config'
    ]
)
cc_library(
    name='generic_waiter',
    srcs=[],
    hdrs=['GenericWaiter.h'],
    include_prefix='sql/common',
    deps=[
        '//aios/autil:lock', '//aios/autil:result', '//aios/autil:thread',
        '//aios/autil:time'
    ]
)
cc_library(
    name='sql_kv_parser',
    srcs=glob(['KvPairParser.cpp']),
    hdrs=glob(['KvPairParser.h']),
    include_prefix='sql/common',
    visibility=['//visibility:public'],
    deps=[':sql_common_h', '//aios/autil:log', '//aios/autil:string_helper']
)
cc_library(
    name='sql_common',
    srcs=glob(['*.cpp'],
              exclude=[
                  'common.cpp', 'TracerAdapter.cpp', 'KvPairParser.cpp',
                  'SqlAuthManager.cpp', 'SqlAuthManagerR.cpp',
                  'IndexPartitionSchemaConverter.cpp',
                  'Ha3TableConverterExec.cpp'
              ]),
    hdrs=glob(['*.h'],
              exclude=[
                  'common.h', 'Log.h', 'TracerAdapter.h', 'KvPairParser.h',
                  'SqlAuthManager.h', 'SqlAuthManagerR.h',
                  'IndexPartitionSchemaConverter.h', 'GenericWaiter.h',
                  'Ha3ClusterDef.h'
              ]),
    include_prefix='sql/common',
    deps=[
        ':schema_converter_lib', ':sql_auth', ':sql_kv_parser', ':sql_tracer',
        '//aios/apps/facility/build_service:bs_hashmode', '//aios/autil:json',
        '//aios/ha3:ha3_util'
    ]
)
cc_library(
    name='schema_converter_lib',
    srcs=glob(['IndexPartitionSchemaConverter.cpp']),
    hdrs=glob(['IndexPartitionSchemaConverter.h']),
    include_prefix='sql/common',
    deps=([
        '//aios/autil:json', '//aios/autil:log',
        '//aios/sql/iquan/cpp/common:iquan_common',
        '//aios/sql/iquan/cpp/common/catalog:iquan_catalog',
        '//aios/storage/indexlib/indexlib/config'
    ] + [])
)
cc_test(
    name='sql_common_test',
    srcs=glob(['test/*Test.cpp']),
    copts=['-fno-access-control'],
    data=[':testdata'],
    deps=[
        ':generic_waiter', ':sql_common',
        '//aios/filesystem/fslib:fslib-framework',
        '//aios/sql/ops/test:ops_testlib', '//aios/unittest_framework'
    ]
)
filegroup(name='testdata', testonly=True, srcs=glob(['test/testdata/**']))
