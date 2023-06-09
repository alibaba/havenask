package(default_visibility=['//visibility:private'])
cc_library(
    name='anet',
    visibility=['//visibility:public'],
    srcs=glob(['*.cpp']),
    hdrs=glob(['*.h']),
    deps=['//aios/alog:alog', '//aios/autil:env_util'],
    copts=['-Wformat-truncation=0']
)
