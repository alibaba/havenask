load('//aios/storage:defs.bzl', 'strict_cc_library')
strict_cc_library(
    name='mock',
    testonly=True,
    srcs=[],
    hdrs=[],
    visibility=['//visibility:public'],
    deps=[':MockDirectory', ':MockFileReader']
)
strict_cc_library(
    name='MockFileReader',
    testonly=True,
    srcs=[],
    deps=['//aios/storage/indexlib/file_system/file:interface']
)
strict_cc_library(
    name='MockDirectory',
    testonly=True,
    srcs=[],
    deps=['//aios/storage/indexlib/file_system:interface']
)
