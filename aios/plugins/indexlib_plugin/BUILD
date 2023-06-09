load(
    '@rules_cc//examples:experimental_cc_shared_library.bzl',
    'cc_shared_library'
)
load('@bazel_skylib//lib:selects.bzl', 'selects')
load('//bazel:defs.bzl', 'cc_proto')
load(
    '//aios/storage/indexlib/index/ann/aitheta2:defs.bzl', 'proxima2_liba_deps'
)
cc_library(
    name='aitheta_indexer_lib',
    srcs=glob([
        'plugins/aitheta/*.cpp', 'plugins/aitheta/index/*.cpp',
        'plugins/aitheta/util/*.cpp',
        'plugins/aitheta/util/embed_holder_iterator/*.cpp',
        'plugins/aitheta/util/parallel_merge/*.cpp',
        'plugins/aitheta/util/knn_params_selector/*.cpp'
    ]),
    hdrs=glob([
        'indexlib_plugin/*.h', 'plugins/aitheta/*.h',
        'plugins/aitheta/index/*.h', 'plugins/aitheta/util/*.h',
        'plugins/aitheta/util/embed_holder_iterator/*.h',
        'plugins/aitheta/util/parallel_merge/*.h',
        'plugins/aitheta/util/knn_params_selector/*.h'
    ]),
    copts=[],
    include_prefix='indexlib_plugin',
    linkopts=['-Bsymbolic'],
    strip_include_prefix='',
    visibility=['//visibility:public'],
    deps=(['//aios/storage/indexlib/indexlib:indexlib'] + select({
        '//:gpu_mode': ['//third_party/proxima/gpu:proxima'],
        '//conditions:default': ['//third_party/proxima/cpu:proxima']
    })),
    alwayslink=1
)
cc_shared_library(
    name='aitheta_indexer',
    preloaded_deps=([
        '@com_google_protobuf//:cc_wkt_protos',
        '@com_google_protobuf//:protobuf',
        '//aios/storage/indexlib/indexlib:indexlib', '@zlib_archive//:zlib'
    ] + select({
        '//:gpu_mode': [
            '@local_config_cuda//cuda:cudart', '@local_config_cuda//cuda:cublas'
        ],
        '//conditions:default': []
    })),
    roots=[':aitheta_indexer_lib'],
    static_deps=([] + select({
        '//:gpu_mode': [
            '//third_party/proxima/gpu:proxima',
            '//third_party/proxima/gpu:proxima_import'
        ],
        '//conditions:default': [
            '//third_party/proxima/cpu:proxima',
            '//third_party/proxima/cpu:proxima_import'
        ]
    })),
    visibility=['//visibility:public']
)
