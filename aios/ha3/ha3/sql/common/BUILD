package(default_visibility=['//aios/ha3:__subpackages__'])
cc_library(
    name='sql_log',
    hdrs=glob(['Log.h']),
    deps=['//aios/autil:log', '//navi:navi'],
    visibility=['//visibility:public'],
    include_prefix='ha3/sql/common'
)
cc_library(
    name='sql_common_h',
    srcs=glob(['common.cpp']),
    hdrs=glob(['common.h']),
    deps=[],
    include_prefix='ha3/sql/common'
)
cc_library(
    name='sql_tracer',
    srcs=glob(['TracerAdapter.cpp']),
    hdrs=glob(['TracerAdapter.h']),
    deps=[
        ':sql_log', ':sql_common_h',
        '@suez_turing.turbojet.turing_ops_util//:turing_ops_util'
    ],
    include_prefix='ha3/sql/common'
)
cc_library(
    name='sql_auth',
    srcs=['SqlAuthManager.cpp'],
    hdrs=['SqlAuthManager.h'],
    deps=[
        ':sql_log', ':sql_common_h', '//aios/ha3/ha3/sql/config:sql_config',
        '//aios/autil:hash_func'
    ],
    include_prefix='ha3/sql/common'
)
cc_library(
    name='generic_waiter',
    srcs=[],
    hdrs=['GenericWaiter.h'],
    deps=[
        '//aios/autil:lock', '//aios/autil:thread', '//aios/autil:result',
        '//aios/autil:time'
    ],
    include_prefix='ha3/sql/common'
)
cc_library(
    name='sql_kv_parser',
    srcs=glob(['KvPairParser.cpp']),
    hdrs=glob(['KvPairParser.h']),
    deps=['//aios/autil:string_helper', '//aios/autil:log', ':sql_common_h'],
    include_prefix='ha3/sql/common',
    visibility=['//visibility:public']
)
cc_library(
    name='sql_common',
    srcs=glob(['*.cpp'],
              exclude=[
                  'common.cpp', 'TracerAdapter.cpp', 'KvPairParser.cpp',
                  'SqlAuthManager.cpp', 'IndexPartitionSchemaConverter.cpp'
              ]),
    hdrs=glob(['*.h'],
              exclude=[
                  'common.h', 'Log.h', 'TracerAdapter.h', 'KvPairParser.h',
                  'SqlAuthManager.h', 'IndexPartitionSchemaConverter.h',
                  'GenericWaiter.h'
              ]),
    deps=[
        ':sql_tracer', ':sql_kv_parser', ':sql_auth', ':schema_converter_lib',
        '//aios/ha3:ha3_util', '//aios/autil:json',
        '//aios/apps/facility/build_service:bs_hashmode'
    ],
    include_prefix='ha3/sql/common'
)
cc_library(
    name='schema_converter_lib',
    srcs=glob(['IndexPartitionSchemaConverter.cpp']),
    hdrs=glob(['IndexPartitionSchemaConverter.h']),
    deps=[
        '//aios/autil:log', '//aios/autil:json',
        '//aios/storage/indexlib/indexlib:indexlib_testlib',
        '//aios/storage/indexlib/table/aggregation:table',
        '//aios/sql/iquan/cpp/common/catalog:iquan_catalog',
        '//aios/sql/iquan/cpp/common:iquan_common'
    ],
    include_prefix='ha3/sql/common'
)
