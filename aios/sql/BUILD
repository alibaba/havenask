package(default_visibility=['//aios/sql:__subpackages__'])
load('//bazel:defs.bzl', 'copy_target_to')
load('//bazel:bundle.bzl', 'bundle_files')
cc_library(
    name='sql_lib',
    visibility=['//visibility:public'],
    deps=([':sql_lib_base', '//aios/sql/framework:sql_framework'] + []),
    alwayslink=1
)
cc_library(
    name='sql_lib_base',
    visibility=['//visibility:public'],
    deps=([
        '//aios/sql/common:sql_common', '//aios/sql/proto:sql_proto',
        '//aios/sql/resource:sql_resource', '//aios/sql/data:sql_data',
        '//aios/sql/config:sql_config', '//aios/sql/ops/util:sql_ops_util',
        '//aios/sql/ops/condition:sql_ops_condition',
        '//aios/sql/ops/scan/kernel:sql_ops_scan',
        '//aios/sql/ops/calc/kernel:sql_ops_calc',
        '//aios/sql/ops/correlate/kernel:sql_ops_correlate',
        '//aios/sql/ops/values/kernel:sql_ops_values',
        '//aios/sql/ops/agg/kernel:sql_ops_agg',
        '//aios/sql/ops/tvf/kernel:sql_ops_tvf',
        '//aios/sql/ops/sort/kernel:sql_ops_sort',
        '//aios/sql/ops/limit/kernel:sql_ops_limit',
        '//aios/sql/ops/union/kernel:sql_ops_union',
        '//aios/sql/ops/sink/kernel:sql_ops_sink',
        '//aios/sql/ops/join/kernel:sql_ops_join',
        '//aios/sql/ops/identity/kernel:sql_ops_identity',
        '//aios/sql/ops/tableModify/kernel:sql_ops_table_modify',
        '//aios/sql/ops/agg/builtin:sql_ops_agg_builtin',
        '//aios/sql/ops/parser/kernel:sql_ops_parser',
        '//aios/sql/ops/planTransform/kernel:sql_ops_plan_transform',
        '//aios/sql/ops/runSqlGraph/kernel:sql_ops_run_sql_graph_kernel',
        '//aios/sql/ops/tableMerge/kernel:sql_ops_table_merge',
        '//aios/sql/ops/tableSplit/kernel:sql_ops_table_split',
        '//aios/sql/ops/delayDp/kernel:sql_ops_delay_dp',
        '//aios/sql/ops/metaCollect:sql_meta_collector',
        '//aios/sql/rpc:sql_rpc'
    ] + [])
)
config_setting(name='compile_with_asan', values={'define': 'compileMode=asan'})
cc_binary(
    name='ha_sql',
    srcs=glob([]),
    linkopts=select({
        ':compile_with_asan': ['-pie', '-mcmodel=large'],
        '//conditions:default': []
    }),
    tags=['fuse-keep-symbol', 'manual', 'no-remote-exec'],
    visibility=['//visibility:public'],
    deps=([
        ':sql_lib', '//aios/suez:suez_worker',
        '//aios/suez_navi:suez_navi_search_manager_creator'
    ] + [
        '//aios/havenask_app_interface:havenask_app_lib',
        '//aios/plugins/havenask_plugins/udf_plugins'
    ])
)
filegroup(name='testdata', srcs=glob(['testdata/**']))
filegroup(
    name='sql_python',
    srcs=glob(['python/*.py'],
              exclude=['local_search_starter.py', 'local_search_stop.py'])
)
filegroup(name='sql_online_script_files', srcs=glob(['tools/py3/utils/*']))
bundle_files(
    name='sql_online_script_package',
    srcs=[':sql_online_script_files'],
    prefix='usr/local/bin',
    strip_prefix='/aios/sql/tools/py3'
)
bundle_files(
    name='sql_etc_package',
    srcs=glob(['misc/*']),
    prefix='usr/local/etc/sql/',
    strip_prefix='misc',
    tags=['manual'],
    visibility=['//visibility:public']
)
bundle_files(
    name='sql_tools_package',
    srcs=glob(['python/*.py']),
    prefix='usr/local/lib/python/site-packages/sql',
    strip_prefix='/aios/sql/python',
    visibility=['//visibility:public'],
    deps=[':sql_online_script_package', '//aios/navi:navi_tools_package']
)
cc_test(
    name='sql_python_test',
    srcs=glob(['python/test/*.cpp']),
    copts=['-fno-access-control'],
    data=[':sql_python', ':testdata'],
    tags=['manual'],
    deps=[
        '//aios/autil:plugin_base', '//aios/filesystem/fslib:fslib-framework',
        '//aios/navi', '//aios/navi:navi_ops_testlib',
        '//aios/unittest_framework'
    ]
)
copy_target_to(
    name='cp_test_tvf_plugin_testdata',
    copy_name='libsql_test_tvf_plugin_lib.so',
    dests=['testdata/sql_tvf/plugins/'],
    to_copy='//aios/sql/ops/tvf/test/plugins:libsql_test_tvf_plugin_lib.so'
)
copy_target_to(
    name='cp_test_tvf_plugin_error_testdata',
    copy_name='libsql_test_tvf_plugin_error_lib.so',
    dests=['testdata/sql_tvf/plugins/'],
    to_copy='//aios/sql/ops/tvf/test/plugins:libsql_test_tvf_plugin_error_lib.so'
)
copy_target_to(
    name='cp_test_tvf_plugin_error_register_testdata',
    copy_name='libsql_test_tvf_plugin_error_register_lib.so',
    dests=['testdata/sql_tvf/plugins/'],
    to_copy='//aios/sql/ops/tvf/test/plugins:libsql_test_tvf_plugin_error_register_lib.so'
)
copy_target_to(
    name='cp_test_agg_plugin_testdata',
    copy_name='libsql_test_agg_plugin_lib.so',
    dests=['testdata/sql_agg_func/plugins/'],
    to_copy='//aios/sql/ops/agg/test/plugins:libsql_test_agg_plugin_lib.so'
)
