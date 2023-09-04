load('//bazel:bundle.bzl', 'bundle_files', 'bundle_install')
bundle_files(
    name='aios_open_source_swift',
    srcs=[],
    deps=['//package/swift:swift_package'],
    visibility=['//visibility:public']
)
bundle_install(
    name='aios_open_source_swift_package',
    srcs=[':aios_open_source_swift'],
    visibility=['//visibility:public']
)
