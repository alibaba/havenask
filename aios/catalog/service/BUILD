load('//bazel:defs.bzl', 'enable_thread_safety')
cc_library(
    name='service',
    srcs=glob(['*.cpp'],
              exclude=['CatalogProtoJsonizer.cpp', 'CatalogWorker.cpp']),
    hdrs=glob(['*.h'], exclude=['CatalogProtoJsonizer.h', 'CatalogWorker.h']),
    copts=enable_thread_safety(),
    include_prefix='catalog/service',
    visibility=['//visibility:public'],
    deps=[
        '//aios/autil:lock', '//aios/autil:log', '//aios/catalog/entity',
        '//aios/catalog/proto:catalog_service_cc_proto', '//aios/catalog/store',
        '//aios/catalog/tools'
    ]
)
