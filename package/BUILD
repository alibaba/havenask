load('//bazel:bundle.bzl', 'bundle_files', 'bundle_py_libs')
bundle_files(
    name='gperftools_package',
    srcs=['@gperftools//:gperftools_lib'],
    visibility=['//visibility:public']
)
bundle_py_libs(
    name='protobuf_python_package',
    prefix='usr/local/lib/python/site-packages/',
    strip_prefixes=['python'],
    srcs=['@com_google_protobuf//:protobuf_python'],
    visibility=['//visibility:public']
)
