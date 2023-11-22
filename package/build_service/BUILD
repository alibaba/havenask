load('//bazel:fuse_debug_bundle.bzl', 'fuse_debug_bundle_files')
load('//bazel:bundle.bzl', 'bundle_files')
bundle_files(
    name='shared_library64_package',
    srcs=([
        '//third_party/lua:lua_shared', '//third_party/lz4:lz4_shared',
        '//third_party/lzma:lzma_shared', '@boringssl//:crypto',
        '@boringssl//:ssl', '//third_party/tbb:tbb_shared',
        '//third_party/uuid:uuid_shared'
    ] + []),
    deps=[
        '//third_party:bhclient_package',
        '//third_party:dadi-cache-sdk_package', '//package:gperftools_package'
    ],
    prefix='usr/local/lib64',
    visibility=['//visibility:public']
)
bundle_files(
    name='shared_library_package',
    srcs=([
        '//third_party/mysql-connector-c++:mysql-connector-cpp_shared',
        '//third_party/zstd:zstd_shared', '@curl//:curl'
    ] + []),
    prefix='usr/local/lib',
    visibility=['//visibility:public']
)
bundle_files(
    name='build_service_tasks',
    srcs=['//aios/apps/facility/build_service/build_service_tasks'],
    prefix='usr/local/lib64',
    visibility=['//visibility:public']
)
bundle_files(
    name='worker_binary',
    srcs=['//aios/apps/facility/build_service:build_service_worker'],
    deps=[
        '//aios/apps/facility/build_service/tools:bs_bin_package',
        '//package/git_commit:commit_version_file'
    ],
    prefix='usr/local/bin'
)
bundle_files(
    name='admin_binary',
    srcs=(['//aios/apps/facility/build_service:bs_admin_worker'] + []),
    deps=([
        '//package/git_commit:commit_version_file',
        '//aios/apps/facility/build_service/tools:bs_bin_package'
    ] + []),
    prefix='usr/local/bin'
)
bundle_files(
    name='tools_binary',
    srcs=[
        '//aios/apps/facility/build_service/tools/general_task_wal_reader:general_task_wal_reader',
        '//aios/apps/facility/build_service/build_service/tools:remote_logger_agent',
        '//aios/apps/facility/build_service/build_service/tools:swift_doc_tracer'
    ],
    deps=['//package/git_commit:commit_version_file'],
    prefix='usr/local/bin'
)
bundle_files(
    name='bs_admin_package',
    visibility=['//visibility:public'],
    srcs=[],
    deps=([
        ':admin_binary', ':shared_library64_package', ':shared_library_package',
        ':tools_binary', '//aios/apps/facility/build_service:bs_etc_package',
        '//aios/apps/facility/build_service:bs_lua_conf_package',
        '//aios/apps/facility/build_service:bs_lua_script_package',
        '//package/git_commit:commit_version_file'
    ] + ['//package/fslib:fslib_package_open_source'])
)
bundle_files(
    name='bs_worker_shared_library_package',
    visibility=['//visibility:public'],
    srcs=[],
    deps=([
        ':bs_protobuf_package', ':build_service_tasks',
        ':shared_library64_package', ':shared_library_package',
        '//aios/apps/facility/build_service:bs_etc_package',
        '//aios/apps/facility/build_service/build_service_tasks:bs_script_task_package',
        '//package/git_commit:commit_version_file'
    ] + ['//package/fslib:fslib_package_open_source'])
)
bundle_files(
    name='bs_worker_package',
    visibility=['//visibility:public'],
    srcs=[],
    deps=([
        ':worker_binary', ':tools_binary', ':bs_worker_shared_library_package'
    ] + [])
)
bundle_files(
    name='bs_protobuf_package',
    srcs=[
        '@com_google_protobuf//:protobuf',
        '@com_google_protobuf//:protobuf_lite'
    ],
    prefix='usr/local/lib64',
    visibility=['//visibility:public']
)
fuse_debug_bundle_files(
    name='bs_package_fused', deps=[':bs_admin_package', ':bs_worker_package']
)
