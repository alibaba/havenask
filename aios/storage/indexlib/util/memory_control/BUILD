load('//aios/storage/indexlib:defs.bzl', 'indexlib_cc_library')
package(default_visibility=['//aios/storage/indexlib:__subpackages__'])
indexlib_cc_library(
    name='memory_control',
    srcs=[
        'BlockMemoryQuotaController.cpp', 'BuildResourceMetrics.cpp',
        'MemoryQuotaSynchronizer.cpp', 'MemoryReserver.cpp', 'QuotaControl.cpp'
    ],
    hdrs=[
        'BlockMemoryQuotaController.h', 'BuildResourceCalculator.h',
        'BuildResourceMetrics.h', 'MemoryQuotaController.h',
        'MemoryQuotaControllerCreator.h', 'MemoryQuotaSynchronizer.h',
        'MemoryReserver.h', 'PartitionMemoryQuotaController.h',
        'QuotaControl.h', 'SimpleMemoryQuotaController.h',
        'WaitMemoryQuotaController.h'
    ],
    deps=[
        '//aios/autil:lock', '//aios/autil:unit_util',
        '//aios/storage/indexlib/base:MemoryQuotaController'
    ]
)
