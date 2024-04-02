load('//bazel:bundle.bzl', 'bundle_files')
bundle_files(
    name='fslib_lib64_package_open_source',
    prefix='usr/local/lib64',
    srcs=[
        '//aios/filesystem/fslib:fslib_plugin_zfs',
        '//aios/filesystem/fslib:fslib_plugin_hdfs',
        '//aios/filesystem/fslib:fslib_plugin_http',
        '//aios/filesystem/fslib:fslib_plugin_oss', '@boringssl//:ssl',
        '@boringssl//:crypto'
    ],
    deps=[':curl_package', ':dfs_lib_package', ':oss_lib_package']
)
bundle_files(
    name='oss_lib_package',
    srcs=['@oss-sdk//:shared_files'],
    visibility=['//visibility:public']
)
bundle_files(
    name='curl_package',
    prefix='usr/local/lib',
    srcs=['@curl//:curl'],
    visibility=['//visibility:public']
)
bundle_files(
    name='dfs_lib_package',
    prefix='usr/local/lib64',
    strip_prefix='src/lib64',
    srcs=['@hdfs-cdh//:hdfs_shared']
)
bundle_files(
    name='fslib_package',
    srcs=[],
    deps=(['//aios/filesystem/fslib/tools:fsutil_tools_package'] +
          [':fslib_lib64_package_open_source']),
    visibility=['//visibility:public']
)
bundle_files(
    name='fslib_package_open_source',
    srcs=[],
    deps=[],
    visibility=['//visibility:public']
)
