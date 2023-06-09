package(default_visibility=['//aios/ha3:__subpackages__'])
cc_library(
    name='message_writer',
    srcs=glob(['*.cpp']),
    hdrs=glob(['*.h']),
    include_prefix='ha3/sql/resource',
    deps=[
        '@swift//:swift', '//aios/ha3/ha3/sql/common:sql_common',
        '//aios/ha3/ha3/sql/common:sql_log',
        '//aios/storage/indexlib:interface',
        '//aios/suez:remote_table_writer_sdk'
    ],
    alwayslink=True
)
