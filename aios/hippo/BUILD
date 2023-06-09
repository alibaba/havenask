package(default_visibility=['//visibility:public'])
load('//bazel:defs.bzl', 'cc_proto')
load('//bazel:bundle.bzl', 'bundle_files')
load('//bazel:defs.bzl', 'gen_conf_lib')
copts = [
    '-Wno-unused-variable', '-Wno-format', '-Wno-deprecated-declarations',
    '-Wno-nonnull-compare'
]
cc_proto(
    name='proto',
    visibility=['//visibility:public'],
    srcs=glob(['hippo/proto/*.proto', 'hippo/proto/yarn/*.proto'],
              exclude=['hippo/proto/HeartBeat.proto']),
    import_prefix='hippo',
    strip_import_prefix='hippo',
    deps=['//aios/network/arpc:proto']
)
test_h = gen_conf_lib(
    name='test_h',
    srcs=['src/test/test.h.in'],
    outs=['src/test/test.h'],
    args={
        '@TOP_SRCDIR@': 'aios/carbon',
        '@TMP_TOP_SRCDIR@': '/tmp/aios/carbon'
    },
    strip_include_prefix='src'
)
test_data = glob(['src/testdata/*', 'src/testdata/**/*'])
