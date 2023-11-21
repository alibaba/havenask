package(default_visibility=['//aios/ha3:__subpackages__'])
load('//bazel:defs.bzl', 'cc_proto', 'gen_conf_lib')
load('@flatbuffers//:build_defs.bzl', 'flatbuffer_library_public')
cc_library(
    name='ha3_util',
    srcs=glob(['ha3/util/*.cpp']),
    hdrs=glob(['ha3/util/*.h']),
    include_prefix='ha3',
    strip_include_prefix='ha3',
    visibility=['//visibility:public'],
    deps=[':ha3_isearch_h', '//aios/autil:compression', '//aios/autil:log']
)
config_setting(name='compile_with_asan', values={'define': 'compileMode=asan'})
gen_conf_lib(
    name='version_h',
    srcs=['ha3/version.h.in'],
    outs=['ha3/version.h'],
    args={
        '@HA_BUILD_SIGNATURE@': 'ha3_3_8_0',
        '@HA_PROTOCOL_VERSION@': 'ha3-protocol-3.0'
    },
    include_prefix='ha3',
    strip_include_prefix='ha3'
)
cc_proto(
    name='ha3_proto_basic_def',
    srcs=['ha3/proto/BasicDefs.proto'],
    import_prefix='ha3/proto/',
    strip_import_prefix='ha3/proto/',
    visibility=['//visibility:public'],
    deps=[]
)
cc_proto(
    name='ha3_proto_rpc',
    srcs=['ha3/proto/QrsService.proto'],
    import_prefix='ha3/proto/',
    strip_import_prefix='ha3/proto/',
    visibility=['//visibility:public'],
    deps=[':ha3_proto_basic_def', '//aios/network/arpc:proto']
)
ha3_fbs_input = [
    'ha3/proto/SqlResult.fbs', 'ha3/proto/SummaryResult.fbs',
    'ha3/proto/TsdbColumn.fbs', 'ha3/proto/TwoDimTable.fbs'
]
filegroup(
    name='ha3_fbs_includes',
    srcs=ha3_fbs_input,
    visibility=['//visibility:public']
)
flatbuffer_library_public(
    name='ha3_fbs',
    srcs=ha3_fbs_input,
    outs=[
        'ha3/proto/SqlResult_generated.h',
        'ha3/proto/SummaryResult_generated.h',
        'ha3/proto/TsdbColumn_generated.h', 'ha3/proto/TwoDimTable_generated.h'
    ],
    language_flag='-c',
    out_prefix='ha3/proto/'
)
cc_library(
    name='ha3_fbs_headers',
    hdrs=[':ha3_fbs'],
    include_prefix='ha3',
    strip_include_prefix='ha3',
    visibility=['//visibility:public'],
    deps=['@flatbuffers']
)
cc_library(
    name='ha3_isearch_h',
    srcs=['ha3/isearch.cpp'],
    hdrs=['ha3/isearch.h'],
    include_prefix='ha3',
    strip_include_prefix='ha3',
    deps=[
        '//aios/autil:long_hash_value', '//aios/matchdoc',
        '//aios/storage/indexlib/indexlib:indexlib_h'
    ]
)
cc_library(
    name='ha3_proto_headers',
    hdrs=glob(['ha3/proto/*.h']),
    include_prefix='ha3',
    strip_include_prefix='ha3',
    visibility=['//visibility:public'],
    deps=[
        ':ha3_isearch_h', ':ha3_proto_basic_def_cc_proto_headers',
        ':ha3_proto_rpc_cc_proto_headers'
    ]
)
cc_library(
    name='ha3_proto',
    srcs=glob(['ha3/proto/*.cpp']),
    visibility=['//visibility:public'],
    deps=[
        ':ha3_proto_basic_def_cc_proto', ':ha3_proto_headers',
        ':ha3_proto_rpc_cc_proto', '//aios/autil:compression'
    ]
)
gen_conf_lib(
    name='ha3_test_h',
    srcs=['ha3/test/test.h.in'],
    outs=['ha3/test/test.h'],
    args={
        '@TOP_SRCDIR@': 'aios/ha3',
        '@TOP_BUILDDIR@': '.',
        '@BUILDDIR@': '.',
        '@INSTALLROOT@': '.'
    },
    include_prefix='ha3',
    strip_include_prefix='ha3'
)
filegroup(
    name='testdata',
    srcs=glob(['testdata/**', 'misc/leak_suppression']),
    data=['leak_suppression']
)
filegroup(name='leak_suppression', srcs=glob(['misc/leak_suppression']))
