load('//bazel:bundle.bzl', 'bundle_files', 'bundle_install')
load('//bazel:fuse_debug_bundle.bzl', 'fuse_debug_bundle_files')
load('//bazel:defs.bzl', 'upload_pkg')
bundle_install(
    name='sql_package',
    srcs=['sql_package_files'],
    visibility=['//visibility:public']
)
bundle_install(
    name='sql_integration_online_package',
    srcs=['sql_integration_online_files'],
    visibility=['//visibility:public']
)
bundle_install(
    name='sql_integration_offline_package',
    srcs=['sql_integration_offline_files', 'sql_integration_swift_files'],
    visibility=['//visibility:public']
)
bundle_install(
    name='sql_performance_test_package',
    srcs=[
        'sql_integration_online_files', 'sql_integration_offline_files',
        'sql_integration_swift_files'
    ],
    visibility=['//visibility:public']
)
upload_pkg(
    name='upload_sql_package',
    target=':sql_online_mini_package_tar',
    oss_prefix='sql_pkg',
    pkg_prefix='sql_'
)
genrule(
    name='jdbc_client_jar',
    srcs=['@jdbc_client_jar//jar'],
    outs=['ha3_jdbc_client.jar'],
    cmd='cp $(location @jdbc_client_jar//jar) $(location ha3_jdbc_client.jar)',
    visibility=['//visibility:public']
)
fuse_debug_bundle_files(
    name='sql_online_mini_package_fused', deps=[':sql_online_mini_package']
)
bundle_files(
    name='sql_open_source_package',
    srcs=[],
    deps=[
        ':sql_binary', ':iquan_client_package',
        '//aios/navi:navi_tools_package', '//aios/sql:sql_tools_package',
        '//aios/sql:sql_etc_package', '//package/suez:suez_admin_package',
        '//package/fslib:fslib_package',
        '//package/git_commit:commit_version_file', '//aios/navi:navi_package'
    ],
    visibility=['//visibility:public']
)
bundle_files(
    name='iquan_client_package',
    prefix='aios/sql/iquan/java',
    srcs=[
        '//aios/sql/iquan/java:iquan_client_jar',
        '//aios/sql/iquan/java:iquan_boot_jar'
    ],
    visibility=['//visibility:public']
)
bundle_files(
    name='sql_binary',
    prefix='usr/local/bin',
    srcs=(['//aios/sql:ha_sql'] + []),
    visibility=['//visibility:public']
)
