package(default_visibility=['//aios/network/opentelemetry:__subpackages__'])
cc_library(
    name='opentelemetry',
    deps=[':opentelemetry_havenask_mock'],
    copts=['-Werror'],
    visibility=['//visibility:public']
)
cc_library(
    name='opentelemetry_havenask_mock',
    hdrs=glob(['havenask_mock/core/*.h', 'havenask_mock/core/eagleeye/*.h']),
    srcs=glob(['havenask_mock/core/*.cpp',
               'havenask_mock/core/eagleeye/*.cpp']),
    deps=['//aios/autil:json'],
    include_prefix='aios/network/opentelemetry/core',
    strip_include_prefix='havenask_mock/core'
)
