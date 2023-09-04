cc_library(
    name='store_impl',
    srcs=[
        'BaseStore.cpp', 'CatalogReader.cpp', 'CatalogState.cpp',
        'CatalogWriter.cpp', 'LocalStore.cpp', 'ZkStore.cpp'
    ],
    hdrs=[
        'BaseStore.h', 'CatalogReader.h', 'CatalogState.h', 'CatalogWriter.h',
        'LocalStore.h', 'ZkStore.h'
    ],
    include_prefix='catalog/store/',
    visibility=['//aios/catalog:__subpackages__'],
    deps=['//aios/catalog/store:interface', '//aios/worker_framework'],
    alwayslink=True
)
cc_library(
    name='store',
    visibility=['//aios/catalog:__subpackages__'],
    deps=([':interface', ':store_impl'] + select({
        '//:disable_catalog_mongodb': [],
        '//conditions:default': ['//aios/catalog/store/mongodb:mongodb_store']
    }))
)
cc_library(
    name='interface',
    srcs=['StoreFactory.cpp'],
    hdrs=['ICatalogReader.h', 'ICatalogWriter.h', 'IStore.h', 'StoreFactory.h'],
    include_prefix='catalog/store',
    visibility=['//aios/catalog:__subpackages__'],
    deps=[
        '//aios/autil:lock', '//aios/catalog/entity',
        '//aios/catalog/proto:catalog_entity_record_cc_proto',
        '//aios/catalog/util'
    ]
)
