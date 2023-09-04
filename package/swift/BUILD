package(default_visibility=['//aios/apps/facility/swift:__subpackages__'])
load('//bazel:bundle.bzl', 'bundle_files', 'bundle_install')
load('//bazel:fuse_debug_bundle.bzl', 'fuse_debug_bundle_files')
fuse_debug_bundle_files(
    name='swift_online_bundle',
    deps=([':swift_package'] + []),
    keep_symbols=([
        '//aios/apps/facility/swift/admin:swift_admin',
        '//aios/apps/facility/swift/broker:swift_broker',
        '//aios/apps/facility/swift/tools/admin_starter:swift_admin_starter',
        '//aios/apps/facility/swift/tools/local_admin_daemon:local_admin_daemon',
        '//aios/apps/facility/swift/tools/swift_diff:swift_diff_bin'
    ] + []),
    compress=True,
    oss_prefix='xxxx://invalid/swift_debug_info_pkg',
    pkg_prefix='swift_debug_info'
)
bundle_install(
    name='swift_dailyrun_test_package',
    srcs=[':swift_dailyrun_package'],
    visibility=['//visibility:public']
)
fuse_debug_bundle_files(
    name='swift_client_bundle',
    deps=['//aios/apps/facility/swift:swift_client_package'],
    keep_symbols=[
        '//aios/apps/facility/swift/client:libswift_client.so',
        '//aios/memory/lockless_allocator:ll_alloc_so'
    ],
    oss_prefix='xxxx://invalid/swift_client_debug_info_pkg',
    pkg_prefix='swift_client_debug_info'
)
bundle_files(
    name='swift_package',
    srcs=['//package/git_commit:commit_version_file'],
    deps=([
        ':shared_library_package',
        '//aios/apps/facility/swift:swift_binary_package',
        '//aios/apps/facility/swift:swift_tools_python_package',
        '//aios/apps/facility/swift:swift_tools_conf_package',
        '//aios/apps/facility/swift:swift_etc_package',
        '//aios/apps/facility/swift:swift_proto_py_package',
        '//aios/network/arpc:arpc_python_package',
        '//package:protobuf_python_package', '//package/fslib:fslib_package'
    ] + []),
    visibility=['//visibility:public']
)
bundle_files(
    name='shared_library_package',
    prefix='usr/local/lib64',
    srcs=[
        '//third_party/lzma:lzma_shared', '//third_party/lz4:lz4_shared',
        '//third_party/tbb:tbb_shared'
    ]
)
