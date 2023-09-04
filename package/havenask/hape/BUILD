load('//bazel:bundle.bzl', 'bundle_install', 'bundle_tar')
bundle_install(
    name='hape_install',
    srcs=['//hape:hape_files'],
    visibility=['//visibility:public']
)
bundle_tar(
    name='hape_tar',
    srcs=['//hape:hape_files'],
    visibility=['//visibility:public']
)
