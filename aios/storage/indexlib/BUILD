load('//bazel:bundle.bzl', 'bundle_files', 'bundle_tar')
load('//aios/storage/indexlib:defs.bzl', 'indexlib_cc_library')
indexlib_cc_library(
    name='tablet',
    srcs=[],
    hdrs=[],
    visibility=['//visibility:public'],
    deps=['//aios/storage/indexlib/framework:tablet']
)
indexlib_cc_library(
    name='document',
    srcs=[],
    hdrs=[],
    visibility=['//visibility:public'],
    deps=['//aios/storage/indexlib/document']
)
indexlib_cc_library(
    name='alter_table',
    srcs=[],
    hdrs=[],
    visibility=['//visibility:public'],
    deps=[
        ':document', '//aios/storage/indexlib/config:schema',
        '//aios/storage/indexlib/document/normal:NormalDocumentParser',
        '//aios/storage/indexlib/framework/index_task:IndexTaskResourceManager',
        '//aios/storage/indexlib/index/attribute:indexer'
    ]
)
indexlib_cc_library(
    name='interface',
    srcs=[],
    hdrs=[],
    visibility=['//visibility:public'],
    deps=[':document', ':tablet']
)
bundle_files(
    name='codegen_package',
    srcs=['//aios/storage/indexlib/legacy/tools:codegen_tool_bin'],
    prefix='usr/local/bin/',
    strip_prefix='codegen_tool',
    visibility=['//visibility:public'],
    deps=[
        '//aios/storage/indexlib/indexlib:codegen_bin_package',
        '//aios/storage/indexlib/indexlib:codegen_clang_include_package',
        '//aios/storage/indexlib/indexlib:codegen_source_code_package'
    ]
)
