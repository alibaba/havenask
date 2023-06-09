cc_library(
    name='table_json',
    hdrs=glob(['table/TableJson.h']),
    deps=['//aios/autil:json'],
    visibility=['//visibility:public'],
    include_prefix='table',
    strip_include_prefix='table'
)
cc_library(
    name='table',
    srcs=glob(['table/*.cpp']),
    hdrs=glob(['table/*.h'], exclude=['table/TableJson.h']),
    deps=[
        'table_json', '//aios/autil:compression', '//aios/autil:log',
        '//aios/autil:string_helper', '//aios/autil:time', '//aios/autil:json',
        '//aios/matchdoc:matchdoc'
    ],
    visibility=['//visibility:public'],
    include_prefix='table',
    strip_include_prefix='table'
)
