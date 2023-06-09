package(default_visibility=['//aios/ha3:__subpackages__'])
cc_library(
    name='ha3_monitor_headers',
    hdrs=glob(['*.h']),
    deps=[
        '//aios/ha3:ha3_isearch_h', '//aios/ha3/ha3/common:ha3_tracer_headers',
        '//aios/ha3:ha3_proto_basic_def_cc_proto_headers',
        '//aios/ha3/ha3/common/searchinfo:ha3_searchinfo_headers',
        '@suez_turing.turbojet.turing_ops_util//:suez_turing',
        '@com_google_protobuf//:protobuf_headers'
    ],
    include_prefix='ha3/monitor'
)
cc_library(
    name='ha3_monitor',
    srcs=glob(['*.cpp']),
    deps=[
        ':ha3_monitor_headers', '//aios/ha3/ha3/common:ha3_tracer',
        '//aios/ha3/ha3/common/searchinfo:ha3_searchinfo_headers',
        '@suez_turing.turbojet.turing_ops_util//:suez_turing'
    ]
)
