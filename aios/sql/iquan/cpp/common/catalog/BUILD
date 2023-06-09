package(default_visibility=['//aios/sql/iquan/cpp:__subpackages__'])
cc_library(
    name='iquan_catalog',
    srcs=glob(['*.cpp'],
              exclude=['IquanCatalogClient.cpp', 'IquanCatalogConvertor.cpp']),
    hdrs=glob(['*.h'],
              exclude=['IquanCatalogClient.h', 'IquanCatalogConvertor.h']),
    copts=['-Werror'],
    deps=['//aios/sql/iquan/cpp/common:iquan_common'],
    visibility=['//visibility:public'],
    include_prefix='iquan/common/catalog'
)
cc_library(
    name='iquan_catalog_client',
    srcs=glob(['IquanCatalogClient.cpp']),
    hdrs=glob(['IquanCatalogClient.h']),
    copts=['-Werror'],
    deps=(['iquan_catalog'] + ['//aios/autil:thread']),
    visibility=['//visibility:public'],
    include_prefix='iquan/common/catalog'
)
