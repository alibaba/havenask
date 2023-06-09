cc_library(
    name='resource_reader',
    srcs=glob(['resource_reader/*.cpp']),
    hdrs=glob(['resource_reader/*.h']),
    deps=[
        '//aios/autil:json', '//aios/autil:log', '//aios/autil:string_helper',
        '//aios/filesystem/fslib:fslib-framework'
    ],
    visibility=['//visibility:public'],
    include_prefix='resource_reader',
    strip_include_prefix='resource_reader'
)
