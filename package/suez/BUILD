load('//bazel:bundle.bzl', 'bundle_files')
bundle_files(
    name='suez_binary_package',
    prefix='usr/local/bin/',
    srcs=['//aios/suez:suez_admin_worker']
)
bundle_files(
    name='suez_shared_lib_package',
    prefix='usr/local/lib64/',
    srcs=([
        '@curl//:curl', '//third_party/lzma:lzma_shared',
        '//third_party/keycenter4c:keycenter4c_shared',
        '//third_party/openssl:crypto_shared',
        '//third_party/openssl:ssl_shared', '//third_party/zookeeper:zookeeper',
        '//third_party/lz4:lz4_shared', '//third_party/zstd:zstd_shared',
        '//third_party/uuid:uuid_shared'
    ] + [
        '//aios/suez:suez_shared_lib',
        '//third_party/elfutils-libelf:elfutils-libelf_shared'
    ]),
    deps=[
        '//third_party:bhclient_package', '//third_party:dadi-cache-sdk_package'
    ]
)
bundle_files(
    name='mongo_shared_library_package',
    srcs=['@mongo-cxx-driver//:shared_files'],
    prefix='usr/local'
)
bundle_files(
    name='suez_admin_package',
    deps=([
        ':suez_shared_lib_package', ':suez_binary_package',
        '//aios/suez:suez_etc_package',
        '//package/git_commit:commit_version_file',
        '//package/fslib:fslib_package'
    ] + [':mongo_shared_library_package']),
    visibility=['//visibility:public']
)
