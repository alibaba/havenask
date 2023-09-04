package(default_visibility=['//aios/apps/facility/cm2:__subpackages__'])
load('//bazel:defs.bzl', 'gen_conf_lib')
load(
    '@rules_cc//examples:experimental_cc_shared_library.bzl',
    'cc_shared_library_permissions'
)
transitive_deps = [
    '//aios/apps/facility/cm2/cm_basic/common:common',
    '//aios/apps/facility/cm2/cm_basic/util:util',
    '//aios/apps/facility/cm2/cm_basic/leader_election:leader_election',
    '//aios/apps/facility/cm2/cm_basic/basic_struct:basic_struct',
    '//aios/apps/facility/cm2/cm_basic/basic_struct:proto_cc_proto',
    '//aios/apps/facility/cm2/cm_basic/basic_struct:proto_cc_impl',
    '//aios/apps/facility/cm2/cm_basic/config:config'
]
cc_shared_library_permissions(
    name='permissions',
    targets=([':cm_basic'] + transitive_deps),
    visibility=['//visibility:public']
)
cc_library(
    name='cm_basic', visibility=['//visibility:public'], deps=transitive_deps
)
gen_conf_lib(
    name='test_h',
    srcs=['unittest/test.h.in'],
    outs=['unittest/test.h'],
    args={
        '@TOP_SRCDIR@': 'aios/apps/facility/cm2/cm_basic',
        '@TOP_BUILDDIR@': '.',
        '@BUILDDIR@': '.',
        '@DOTEST_LOGGER_CONF@': '',
        '@INSTALLROOT@': '.'
    }
)
