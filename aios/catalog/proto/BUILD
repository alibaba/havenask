load('//bazel:defs.bzl', 'cc_proto')
load('//bazel:defs.bzl', 'cc_and_py_proto')
cc_and_py_proto(
    name='catalog_entity',
    srcs=['CatalogEntity.proto'],
    import_prefix='catalog/proto',
    visibility=['//visibility:public'],
    deps=[]
)
cc_proto(
    name='catalog_service',
    srcs=['CatalogService.proto'],
    import_prefix='catalog/proto',
    visibility=['//visibility:public'],
    deps=[':catalog_entity', '//aios/network/arpc:proto']
)
cc_proto(
    name='catalog_entity_record',
    srcs=['CatalogEntityRecord.proto'],
    import_prefix='catalog/proto',
    visibility=['//visibility:public'],
    deps=[':catalog_entity']
)
