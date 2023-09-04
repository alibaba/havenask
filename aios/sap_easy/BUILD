package(default_visibility=['//visibility:private'])
load('//bazel:defs.bzl', 'gen_conf_lib')
cc_library(
    name='sap_easy_util',
    hdrs=['sap/common/common.h'],
    include_prefix='sap',
    strip_include_prefix='sap',
    visibility=['//visibility:public']
)
gen_conf_lib(
    name='buildinfo',
    srcs=['sap/framework/buildinfo.h.in'],
    outs=['sap/framework/buildinfo.h'],
    args=[],
    include_prefix='sap',
    strip_include_prefix='sap'
)
gen_conf_lib(
    name='test_h',
    srcs=['unittest/test.h.in'],
    outs=['unittest/test.h'],
    args={
        '@TOP_SRCDIR@': 'aios/sap_easy',
        '@DOTEST_LOGGER_CONF@': 'aios/sap_easy/logger.conf'
    },
    include_prefix='sap',
    strip_include_prefix='.'
)
