package(default_visibility=['//aios/ha3:__subpackages__'])
cc_library(
    name='sql_graph_transform',
    srcs=glob(['*.cpp']),
    hdrs=glob(['*.h']),
    deps=[
        '//aios/sql/iquan/cpp/common:iquan_common',
        '//aios/sql/iquan/cpp/jni:iquan_jni',
        '//aios/ha3/ha3/sql/common:sql_common', '//navi:navi'
    ],
    include_prefix='ha3/sql/ops/planTransform',
    alwayslink=True
)
