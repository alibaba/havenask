cc_library(
    name='alog',
    srcs=glob(['src/cpp/*.cpp', 'src/cpp/*.h']),
    hdrs=glob(['include/alog/*.h']),
    copts=['-Wformat-truncation=0'],
    strip_include_prefix='include',
    visibility=['//visibility:public'],
    deps=['@zlib_archive//:zlib']
)
