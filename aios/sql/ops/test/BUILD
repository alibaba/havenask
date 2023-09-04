package(default_visibility=['//aios/sql:__subpackages__'])
cc_library(
    name='ops_testlib',
    testonly=True,
    srcs=glob(['OpTestBase.cpp']),
    hdrs=glob(['OpTestBase.h']),
    copts=['-fno-access-control'],
    include_prefix='sql/ops/test/',
    visibility=['//visibility:public'],
    deps=[
        '//aios/filesystem/fslib:fslib-framework', '//aios/matchdoc',
        '//aios/navi', '//aios/navi:navi_ops_testlib',
        '//aios/sql/data:sql_data',
        '//aios/sql/ops/agg:sql_ops_agg_func_factory',
        '//aios/sql/ops/agg/builtin:sql_ops_agg_builtin',
        '//aios/sql/ops/scan/udf_to_query:sql_ops_udf_to_query',
        '//aios/sql/ops/tvf:sql_ops_tvf_factory',
        '//aios/sql/ops/tvf/builtin:sql_ops_tvf_builtin',
        '//aios/sql/resource:sql_resource',
        '//aios/sql/resource/testlib:mock_tablet_manager_r',
        '//aios/storage/indexlib/indexlib:indexlib_testlib',
        '//aios/suez_turing/suez/turing/expression:turing_expression_syntax',
        '//aios/table/table/test:table_testlib',
        '//aios/unittest_framework:unittest_lib'
    ],
    alwayslink=True
)
