package(default_visibility=['//aios/sql:__subpackages__'])
cc_library(
    name='sql_meta_collector',
    srcs=glob(['*.cpp']),
    hdrs=glob(['*.h']),
    include_prefix='sql/ops/metaCollect',
    deps=['//aios/navi', '//aios/sql/proto:sql_proto'],
    alwayslink=True
)
