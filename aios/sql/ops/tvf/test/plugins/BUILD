package(default_visibility=['//aios/sql:__subpackages__'])
load(
    '@rules_cc//examples:experimental_cc_shared_library.bzl',
    'cc_shared_library'
)
cc_library(
    name='libsql_test_tvf_plugin_lib',
    srcs=glob(['EchoTvfFunctionCreatorFactory.cpp', 'EchoTvfFunc.cpp', '*.h']),
    deps=['//aios/sql/ops/tvf:sql_ops_tvf_base', '//aios/table']
)
cc_shared_library(
    name='libsql_test_tvf_plugin_lib.so',
    preloaded_deps=[
        '//aios/sql/ops/tvf:sql_ops_tvf_base', '//aios/table:table'
    ],
    roots=[':libsql_test_tvf_plugin_lib'],
    static_deps=[]
)
cc_library(
    name='libsql_test_tvf_plugin_error_lib',
    srcs=glob(['EchoTvfFunctionCreatorFactoryError.cpp', 'EchoTvfFunc.cpp']),
    hdrs=glob(['*.h']),
    deps=['//aios/sql/ops/tvf:sql_ops_tvf_base', '//aios/table']
)
cc_shared_library(
    name='libsql_test_tvf_plugin_error_lib.so',
    preloaded_deps=[
        '//aios/sql/ops/tvf:sql_ops_tvf_base', '//aios/table:table'
    ],
    roots=[':libsql_test_tvf_plugin_error_lib'],
    static_deps=[]
)
cc_library(
    name='libsql_test_tvf_plugin_error_register_lib',
    srcs=glob([
        'EchoTvfFunctionCreatorFactoryErrorRegister.cpp', 'EchoTvfFunc.cpp'
    ]),
    hdrs=glob(['*.h']),
    deps=['//aios/sql/ops/tvf:sql_ops_tvf_base', '//aios/table']
)
cc_shared_library(
    name='libsql_test_tvf_plugin_error_register_lib.so',
    preloaded_deps=[
        '//aios/sql/ops/tvf:sql_ops_tvf_base', '//aios/table:table'
    ],
    roots=[':libsql_test_tvf_plugin_error_register_lib'],
    static_deps=[]
)
