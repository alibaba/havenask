package(default_visibility=['//aios/sql:__subpackages__'])
cc_library(
    name='sql_resource',
    srcs=glob(['*.cpp']),
    hdrs=glob(['*.h']),
    include_prefix='sql/resource',
    deps=[
        '//aios/autil:plugin_base', '//aios/autil:result',
        '//aios/autil:shared_object_map', '//aios/autil:timeout_terminator',
        '//aios/ha3:ha3_util', '//aios/ha3/ha3/common:common_def',
        '//aios/ha3/ha3/common/query:ha3_query',
        '//aios/kmonitor:metrics_report_navi_resource', '//aios/navi',
        '//aios/resource_reader', '//aios/sql/common:sql_common',
        '//aios/sql/common:sql_log', '//aios/sql/config:sql_config',
        '//aios/sql/iquan/cpp/jni:iquan_jni',
        '//aios/sql/ops/agg:sql_ops_agg_func_factory',
        '//aios/sql/ops/tvf:sql_ops_tvf_factory', '//aios/sql/proto:sql_proto',
        '//aios/sql/resource/message_writer', '//aios/sql/resource/watermark',
        '//aios/storage/indexlib:interface',
        '//aios/suez:remote_table_writer_sdk',
        '//aios/suez_turing/suez/turing/expression:turing_expression',
        '//aios/suez_turing/suez/turing/expression:turing_expression_navi_resource',
        '//aios/suez_turing/suez/turing/navi:core'
    ],
    alwayslink=True
)
cc_test(
    name='sql_resource_test',
    srcs=glob(['test/*Test.cpp'], exclude=[]),
    copts=['-fno-access-control'],
    data=[
        '//aios/filesystem/fslib:fslib_plugin_hdfs', '//aios/sql:testdata',
        '//aios/sql/iquan/java:iquan_client_jar'
    ],
    deps=[
        ':sql_resource', '//aios/filesystem/fslib:fslib-framework',
        '//aios/filesystem/fslib/fslib/fs/zookeeper:fslib_zookeeper_casehelper',
        '//aios/navi:navi_log_testlib', '//aios/navi:navi_ops_testlib',
        '//aios/unittest_framework'
    ]
)
