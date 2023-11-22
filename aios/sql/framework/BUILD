package(default_visibility=['//aios/sql:__subpackages__'])
load('@flatbuffers//:build_defs.bzl', 'flatbuffer_library_public')
cc_library(
    name='sql_framework',
    include_prefix='sql/framework',
    deps=[':push_down_op', ':result_formatter', ':sql_access_log'],
    alwayslink=True
)
cc_library(
    name='push_down_op',
    srcs=[],
    hdrs=['PushDownOp.h'],
    include_prefix='sql/framework',
    alwayslink=True
)
cc_library(
    name='sql_access_log',
    srcs=[
        'SqlAccessLog.cpp', 'SqlAccessLogFormatHelper.cpp',
        'SqlErrorAccessLog.cpp', 'SqlSlowAccessLog.cpp'
    ],
    hdrs=[
        'SqlAccessLog.h', 'SqlAccessLogFormatHelper.h', 'SqlErrorAccessLog.h',
        'SqlSlowAccessLog.h'
    ],
    include_prefix='sql/framework',
    deps=[
        '//aios/ha3/ha3/common:ha3_error_result',
        '//aios/network/gig:multi_call', '//aios/sql/common:sql_common',
        '//aios/sql/proto:sql_proto'
    ],
    alwayslink=True
)
cc_library(
    name='result_formatter',
    srcs=['ResultFormatter.cpp'],
    hdrs=['ResultFormatter.h'],
    include_prefix='sql/framework',
    deps=[':open_source_ops', ':sql_result_util'],
    alwayslink=True
)
cc_library(name='open_source_ops', deps=[])
cc_library(
    name='sql_result_util',
    srcs=['SqlResultFormatter.cpp', 'SqlResultUtil.cpp'],
    hdrs=[
        'QrsSessionSqlRequest.h', 'QrsSessionSqlResult.h', 'SqlQueryResponse.h',
        'SqlResultFormatter.h', 'SqlResultUtil.h'
    ],
    include_prefix='sql/framework',
    deps=[
        ':sql_access_log', '//aios/ha3:ha3_fbs_headers', '//aios/matchdoc',
        '//aios/sql/data:sql_data'
    ],
    alwayslink=True
)
cc_library(
    name='sql_push_down_op',
    srcs=[],
    hdrs=['PushDownOp.h'],
    include_prefix='sql/framework',
    deps=[],
    alwayslink=True
)
cc_test(
    name='sql_access_log_test',
    srcs=['test/SqlAccessLogFormatHelperTest.cpp', 'test/SqlAccessLogTest.cpp'],
    copts=['-fno-access-control'],
    deps=[':sql_access_log', '//aios/unittest_framework']
)
cc_test(
    name='sql_result_test_test',
    srcs=[
        'test/SqlResultFormatterPerfTest.cpp',
        'test/SqlResultFormatterTest.cpp', 'test/SqlResultUtilTest.cpp'
    ],
    copts=['-fno-access-control'],
    deps=[
        ':fake_fbs_headers', ':sql_result_util',
        '//aios/autil/autil/legacy/test:json_testlib',
        '//aios/ha3:ha3_fbs_headers', '//aios/table/test:table_testlib',
        '//aios/unittest_framework'
    ]
)
flatbuffer_library_public(
    name='fake_fbs',
    srcs=['test/FakeSqlResult.fbs'],
    outs=['test/FakeSqlResult_generated.h'],
    include_paths=['aios/ha3/ha3/proto'],
    includes=['//aios/ha3:ha3_fbs_includes'],
    language_flag='--include-prefix ha3/proto -c',
    out_prefix='test'
)
cc_library(
    name='fake_fbs_headers',
    testonly=True,
    hdrs=[':fake_fbs'],
    include_prefix='sql/framework/test',
    strip_include_prefix='test',
    visibility=['//visibility:public'],
    deps=['@flatbuffers']
)
