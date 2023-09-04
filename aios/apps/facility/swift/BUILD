package(default_visibility=['//aios/apps/facility/swift:__subpackages__'])
load('//bazel:defs.bzl', 'gen_conf_lib')
load('//bazel:defs.bzl', 'hdrs_pkg')
load('//bazel:bundle.bzl', 'bundle_files')
cc_library(
    name='swift_client',
    visibility=['//visibility:public'],
    deps=[
        '//aios/apps/facility/swift/client:swift_client_lib',
        '//aios/apps/facility/swift/client:swift_client_public_headers',
        '//aios/apps/facility/swift/common:swift_common_public_headers',
        '//aios/apps/facility/swift/network:swift_network_public_headers',
        '//aios/apps/facility/swift/protocol:swift_protocol_public_headers'
    ]
)
hdrs_pkg(
    name='swift_jna_headers_package',
    srcs=['//aios/apps/facility/swift/client/jna:swift_client_jna_headers'],
    visibility=['//visibility:public']
)
gen_conf_lib(
    name='swift_version_h',
    srcs=['version.h.in'],
    outs=['version.h'],
    args={
        '@SWIFT_CLIENT_VERSION@': '1.13.0',
        '@SWIFT_ADMIN_VERSION@': '1.13.0',
        '@SWIFT_BROKER_VERSION@': '1.13.0'
    },
    include_prefix='swift'
)
gen_conf_lib(
    name='swift_test_h',
    srcs=['test/test.h.in'],
    outs=['test/test.h'],
    args={
        '@TOP_SRCDIR@': 'swift',
        '@DOTEST_LOGGER_CONF@': ''
    }
)
filegroup(name='swift_testdata', testonly=True, srcs=glob(['testdata/**']))
bundle_files(
    name='swift_binary_package',
    srcs=([
        '//aios/apps/facility/swift/admin:swift_admin',
        '//aios/apps/facility/swift/broker:swift_broker',
        '//aios/apps/facility/swift/tools/config_validator:swift_config_validator',
        '//aios/apps/facility/swift/tools/admin_starter:swift_admin_starter',
        '//aios/apps/facility/swift/tools/local_admin_daemon'
    ] + []),
    prefix='usr/local/bin/',
    tags=['manual'],
    visibility=['//visibility:public'],
    deps=['//aios/apps/facility/swift/py_tools:swift_bin']
)
bundle_files(
    name='swift_tools_python_package',
    srcs=['//aios/apps/facility/swift/py_tools:swift_tools'],
    prefix='usr/local/lib/python/site-packages',
    tags=['manual'],
    visibility=['//visibility:public']
)
bundle_files(
    name='swift_tools_conf_package',
    srcs=['//aios/apps/facility/swift/py_tools:swift_tools_conf'],
    prefix='usr/local/',
    tags=['manual'],
    visibility=['//visibility:public']
)
bundle_files(
    name='swift_etc_package',
    srcs=['//aios/apps/facility/swift:swift_misc'],
    prefix='usr/local/etc/swift/',
    strip_prefix='misc/',
    visibility=['//visibility:public']
)
bundle_files(
    name='swift_proto_py_package',
    srcs=[],
    prefix='usr/local/lib/python/site-packages/swift/',
    tags=['manual'],
    visibility=['//visibility:public'],
    deps=[':swift_protocol_py_package', ':swift_python_init_package']
)
bundle_files(
    name='swift_python_init_package',
    srcs=['//aios/network/arpc:py_init_file'],
    prefix='usr/local/lib/python/site-packages/swift/',
    strip_prefix='arpc/python'
)
bundle_files(
    name='swift_protocol_init_package',
    srcs=['//aios/network/arpc:py_init_file'],
    prefix='usr/local/lib/python/site-packages/swift/protocol',
    strip_prefix='arpc/python'
)
bundle_files(
    name='swift_protocol_py_package',
    srcs=['//aios/apps/facility/swift/protocol:swift_proto_py'],
    prefix='usr/local/lib/python/site-packages/swift/protocol/',
    tags=['manual'],
    deps=[':swift_protocol_init_package']
)
filegroup(
    name='swift_misc',
    srcs=[
        'misc/apsara_log_conf.json', 'misc/ip_seg_map', 'misc/leak_suppression',
        'misc/swift_alog.conf', 'misc/swift_client_alog.conf'
    ]
)
