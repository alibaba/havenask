cc_library(
    name='alog',
    visibility=['//visibility:public'],
    srcs=glob(['src/cpp/*.cpp', 'src/cpp/*.h']),
    hdrs=glob(['include/alog/*.h']),
    deps=['@zlib_archive//:zlib'],
    copts=['-Wformat-truncation=0'],
    strip_include_prefix='include'
)
