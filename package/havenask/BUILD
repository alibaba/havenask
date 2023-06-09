load('@org_tensorflow//third_party/mkl:build_defs.bzl', 'if_mkl')
load(
    '//bazel:bundle.bzl', 'bundle_files', 'bundle_cc_hdrs', 'bundle_tar',
    'bundle_install'
)
load('//bazel:fuse_debug_bundle.bzl', 'fuse_debug_bundle_files')
load('//bazel:defs.bzl', 'copy_target_to')
bundle_tar(
    name='havenask_package_tar',
    extension='tar',
    srcs=['havenask_package_files'],
    visibility=['//visibility:public']
)
bundle_install(
    name='havenask_package',
    srcs=[':havenask_package_files'],
    visibility=['//visibility:public']
)
bundle_files(
    name='havenask_package_files',
    srcs=[],
    deps=[
        ':havenask_binary', ':bs_binary', '//aios/ha3:ha_tools_package',
        '//aios/apps/facility/build_service:bs_tools_python_package',
        '//aios/apps/facility/build_service:bs_tools_etc_package',
        '//aios/apps/facility/build_service/tools:bs_bin_package',
        '//aios/filesystem/fslib/tools:fsutil_tools_package',
        '//aios/ha3:ha3_etc_package', ':shared_library_package',
        ':havenask_plugin_package', '@iquan_client//:iquan_client_package',
        '@org_tensorflow//:lib_package', '@cm2//:lib_package',
        '@swift//:lib_package',
        '@suez_turing.turbojet.turing_ops_util//:lib_package'
    ],
    visibility=['//visibility:public']
)
bundle_files(
    name='havenask_plugin_package',
    prefix='usr/local/lib64',
    srcs=['//aios/plugins/indexlib_plugin:aitheta_indexer'],
    deps=[
        '//aios/plugins/havenask_plugins/udf_plugins:test_udf_plugins_package',
        '//aios/plugins/havenask_plugins/build_plugins:processor_example_package',
        '//aios/plugins/havenask_plugins/analyzer_plugins:analyzer_plugin_package',
        '//aios/plugins/havenask_plugins/bs_reader_plugins:bs_reader_plugins_package'
    ]
)
bundle_files(
    name='shared_library_package',
    prefix='usr/local/lib64',
    srcs=[
        '//aios/filesystem/fslib:fslib_plugin_hdfs',
        '//aios/filesystem/fslib:fslib_plugin_http',
        '//third_party/zstd:zstd_shared', '//third_party/lz4:lz4_shared',
        '//third_party/tbb:tbb_shared',
        '//third_party/elfutils-libelf:elfutils-libelf_shared',
        '@jvm_libs//:jvm_so', '@rt_libs//:librt.so'
    ],
    deps=[':dfs_lib_package', ':rdkafkacpp_package']
)
bundle_files(
    name='dfs_lib_package',
    prefix='usr/local/lib64',
    strip_prefix='src/lib64',
    srcs=['@hdfs-cdh//:hdfs_shared']
)
bundle_files(
    name='rdkafkacpp_package',
    prefix='usr/local/lib64',
    strip_prefix='rdkafka_2_0_2_libs/lib',
    srcs=['@rdkafkacpp//:rdkafkacpp_shared']
)
bundle_files(
    name='bs_binary',
    prefix='usr/local/bin',
    srcs=[
        '//aios/apps/facility/build_service/build_service/tools:bs_generation_scanner',
        '//aios/apps/facility/build_service:bs_local_job'
    ],
    visibility=['//visibility:public']
)
bundle_files(
    name='havenask_binary',
    prefix='usr/local',
    srcs=[':havenask_sap_server_d'],
    visibility=['//visibility:public']
)
copy_target_to(
    name='havenask_sap_server_d',
    to_copy='//aios/ha3:havenask',
    copy_name='sap_server_d',
    dests=['bin/']
)
