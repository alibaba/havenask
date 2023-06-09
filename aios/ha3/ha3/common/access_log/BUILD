package(default_visibility=['//aios/ha3:__subpackages__'])
cc_library(
    name='access_log_headers',
    hdrs=glob(['*.h']),
    include_prefix='ha3/common/access_log'
)
cc_library(
    name='access_log',
    srcs=glob(['*.cpp']),
    deps=[':access_log_headers', '//aios/autil:log', '//aios/autil:time']
)
