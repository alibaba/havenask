package(default_visibility=['//aios/plugins/havenask_plugins:__subpackages__'])
load('//bazel:defs.bzl', 'gen_conf_lib')
cc_library(
    name='udf_plugins',
    srcs=glob([
        'HavenaskUdfFactory.cpp', 'HavenaskUdfFactoryR.cpp',
        'relevance/**/*.cpp', 'vectorscore/*.cpp', 'sumof/*.cpp'
    ]),
    hdrs=glob([
        'HavenaskUdfFactory.h', 'HavenaskUdfFactoryR.h', 'relevance/**/*.h',
        'vectorscore/*.h', 'sumof/*.h'
    ]),
    deps=[
        ':udf_plugins_common',
        '//aios/suez_turing/suez/turing/expression:turing_expression',
        '//aios/sql/ops/scan:sql_ops_normal_scan'
    ],
    alwayslink=True,
    visibility=['//visibility:public'],
    copts=['-fpermissive'],
    include_prefix='havenask_plugins/udf_plugins'
)
cc_library(
    name='udf_plugins_common',
    srcs=glob(['utils/*.cpp']),
    hdrs=glob(['utils/*.h']),
    deps=[
        '//aios/suez_turing:common_define', '//aios/sql/common:sql_log',
        '//aios/suez_turing/suez/turing/expression:turing_expression',
        '//aios/ha3:ha3_proto_basic_def_cc_proto_headers',
        '//aios/ha3/ha3/search/auxiliary_chain:ha3_auxiliary_chain',
        '//aios/ha3/ha3/search/single_layer_searcher:ha3_single_layer_searcher',
        '//aios/suez_turing/suez/turing/expression:turing_expression_navi_resource'
    ],
    alwayslink=True,
    visibility=['//visibility:public'],
    include_prefix='havenask_plugins/udf_plugins'
)
gen_conf_lib(
    name='test_h',
    srcs=['test/test.h.in'],
    outs=['test/test.h'],
    args={
        '@TOP_SRCDIR@': 'aios/plugins/havenask_plugins/udf_plugins',
        '@TOP_BUILDDIR@': '.',
        '@BUILDDIR@': '.',
        '@INSTALLROOT@': '.'
    },
    include_prefix='test',
    strip_include_prefix='test'
)
