load('//bazel:defs.bzl', 'cc_proto')
load('//aios/storage/indexlib:defs.bzl', 'indexlib_cc_library')
package(default_visibility=['//aios/storage/indexlib:__subpackages__'])
cc_proto(
    name='querier_proto',
    srcs=glob(['*.proto']),
    import_prefix='indexlib/base/proto',
    visibility=['//visibility:public'],
    deps=[]
)
indexlib_cc_library(
    name='querier_proto_headers',
    srcs=[],
    hdrs=glob(['*.h']),
    include_prefix='indexlib/base/proto',
    deps=[':querier_proto_cc_proto_headers']
)
indexlib_cc_library(
    name='querier_proto_base',
    srcs=glob(['*.cpp']),
    hdrs=[],
    deps=[':querier_proto_cc_proto', ':querier_proto_headers']
)
