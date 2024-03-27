cc_library(
    name='curl_client_lib',
    hdrs=glob(['*.h']),
    srcs=glob(['*.cpp']),
    deps=[
        '@curl//:curl', '@rapidjson//:rapidjson', '//aios/autil:log',
        '//aios/autil:string_helper', '//aios/autil:json_helper',
        '//aios/autil:thread', '//aios/autil:resource_pool'
    ],
    visibility=['//visibility:public'],
    include_prefix='curl_client'
)
