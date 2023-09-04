cc_library(
    name='util',
    srcs=glob(['*.cpp']),
    hdrs=glob(['*.h']),
    include_prefix='catalog/util',
    visibility=['//aios/catalog:__subpackages__'],
    deps=[
        '//aios/autil:log', '//aios/autil:string_helper',
        '//aios/catalog/proto:catalog_entity_cc_proto',
        '//aios/catalog/proto:catalog_service_cc_proto',
        '//aios/filesystem/fslib:fslib-framework'
    ]
)
