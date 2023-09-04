cc_library(
    name='entity',
    srcs=glob(['*.cpp']),
    hdrs=glob(['*.h']),
    include_prefix='catalog/entity',
    visibility=['//aios/catalog:__subpackages__'],
    deps=[
        '//aios/autil:log', '//aios/autil:string_helper',
        '//aios/catalog/proto:catalog_entity_cc_proto',
        '//aios/catalog/proto:catalog_entity_record_cc_proto',
        '//aios/catalog/proto:catalog_service_cc_proto', '//aios/catalog/util'
    ]
)
