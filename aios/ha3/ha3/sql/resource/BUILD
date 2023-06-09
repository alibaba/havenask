package(default_visibility=['//aios/ha3:__subpackages__'])
cc_library(
    name='sql_meta_info_resource',
    srcs=glob(['MetaInfoResource.cpp']),
    hdrs=glob(['MetaInfoResource.h']),
    include_prefix='ha3/sql/resource',
    deps=['//aios/ha3/ha3/sql/proto:sql_proto', '//navi'],
    alwayslink=True
)
cc_library(
    name='sql_resource',
    srcs=glob(['*.cpp'], exclude=['MetaInfoResource.cpp']),
    hdrs=glob(['*.h'], exclude=['MetaInfoResource.h']),
    include_prefix='ha3/sql/resource',
    deps=[
        ':sql_meta_info_resource', '//aios/autil:plugin_base',
        '//aios/autil:result', '//aios/autil:shared_object_map',
        '//aios/autil:timeout_terminator',
        '//aios/ha3/ha3/common/query:ha3_query',
        '//aios/ha3/ha3/sql/common:sql_common',
        '//aios/ha3/ha3/sql/common:sql_log',
        '//aios/ha3/ha3/sql/config:sql_config',
        '//aios/ha3/ha3/sql/proto:sql_proto',
        '//aios/ha3/ha3/sql/resource/message_writer',
        '//aios/ha3/ha3/sql/resource/watermark',
        '//aios/ha3/ha3/turing/common/metadata:modelconfig',
        '//aios/ha3/ha3/turing/common/metadata:sort_descriptions',
        '//aios/resource_reader',
        '//aios/sql/iquan/cpp/common/catalog:iquan_catalog_client',
        '//aios/sql/iquan/cpp/jni:iquan_jni',
        '//aios/storage/indexlib:interface',
        '//aios/suez:remote_table_writer_sdk',
        '@suez_turing.turbojet.turing_ops_util//:suez_turing', '//navi'
    ],
    alwayslink=True
)
