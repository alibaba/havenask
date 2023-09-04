cc_library(
    name='tools',
    srcs=['BSConfigMaker.cpp', 'ConfigFileUtil.cpp', 'TableSchemaConfig.cpp'],
    hdrs=['BSConfigMaker.h', 'ConfigFileUtil.h', 'TableSchemaConfig.h'],
    include_prefix='catalog/tools',
    visibility=['//visibility:public'],
    deps=[
        '//aios/autil:log', '//aios/autil:result', '//aios/catalog/entity',
        '//aios/catalog/proto:catalog_entity_cc_proto', '//aios/catalog/util'
    ]
)
