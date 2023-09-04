package(default_visibility=['//aios/suez:__subpackages__'])
load('//bazel:bundle.bzl', 'bundle_files')
filegroup(name='misc', srcs=glob(['misc/*.conf']))
bundle_files(
    name='suez_etc_package',
    srcs=['//aios/suez:misc'],
    prefix='usr/local/etc/suez/',
    strip_prefix='misc',
    visibility=['//visibility:public']
)
cc_library(
    name='suez_index_provider_r',
    visibility=['//visibility:public'],
    deps=['//aios/suez/sdk:index_provider_r']
)
cc_library(
    name='suez_search_sdk',
    visibility=['//visibility:public'],
    deps=['//aios/suez/sdk'],
    alwayslink=1
)
cc_library(
    name='remote_table_writer_sdk',
    visibility=['//visibility:public'],
    deps=['//aios/suez/sdk:remote_table_writer']
)
cc_binary(
    name='suez_admin_worker',
    tags=['manual'],
    visibility=[
        '//aios/igraph:__pkg__', '//package/abfs:__pkg__',
        '//package/abfs/online:__pkg__', '//package/be/online:__pkg__',
        '//package/dolphin/online:__pkg__', '//package/ha3:__pkg__',
        '//package/havenask:__pkg__', '//package/rtp/online:__pkg__',
        '//package/sql:__pkg__', '//package/suez:__pkg__',
        '//package/torch_turing:__pkg__'
    ],
    deps=[
        '//aios/suez/admin:suez_admin',
        '//aios/worker_framework:worker_base_main'
    ]
)
cc_library(
    name='suez_worker',
    visibility=[
        '//aios/ha3:__pkg__', '//aios/sql:__subpackages__',
        '//aios/igraph:__pkg__', '//abfs_turing:__pkg__', '//be_turing:__pkg__',
        '//d2_framework:__pkg__', '//rtp_turing:__pkg__',
        '//torch_turing:__pkg__', '//aios/apps/engine/dolphin:__pkg__',
        '//aios/havenask_app_interface:__pkg__', '//aios/suez:__subpackages__',
        '//integration_test/storage_local:__subpackages__',
        '//integration_test/suez:__subpackages__',
        '//aios/ha3/ha3/turing/common/test:__subpackages__'
    ],
    deps=['//aios/suez/worker:suez_worker_lib'],
    alwayslink=1
)
filegroup(name='testdata', srcs=glob(['testdata/**']))
bundle_files(
    name='suez_shared_lib',
    srcs=[
        '//third_party/keycenter4c:keycenter4c_shared',
        '//third_party/openssl:crypto_shared',
        '//third_party/openssl:ssl_shared'
    ],
    prefix='usr/local/lib64',
    visibility=['//visibility:public'],
    deps=[':mongo_shared_library']
)
bundle_files(
    name='mongo_shared_library',
    srcs=['@mongo-cxx-driver//:shared_files'],
    prefix='usr/local'
)
