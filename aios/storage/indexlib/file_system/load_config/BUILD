load('//aios/storage/indexlib:defs.bzl', 'indexlib_cc_library')
package(
    default_visibility=['//aios/storage/indexlib/file_system:__subpackages__']
)
indexlib_cc_library(
    name='interface',
    srcs=[],
    hdrs=[
        'LoadConfig.h', 'LoadConfigList.h', 'LoadStrategy.h', 'WarmupStrategy.h'
    ],
    visibility=['//visibility:public'],
    deps=['//aios/storage/indexlib/util:RegularExpression']
)
indexlib_cc_library(
    name='load_config',
    srcs=[
        'CacheLoadStrategy.cpp', 'LoadConfig.cpp', 'LoadConfigList.cpp',
        'LoadStrategy.cpp', 'MemLoadStrategy.cpp', 'MmapLoadStrategy.cpp',
        'WarmupStrategy.cpp'
    ],
    hdrs=['CacheLoadStrategy.h', 'MemLoadStrategy.h', 'MmapLoadStrategy.h'],
    deps=[
        ':interface', '//aios/autil:json', '//aios/autil:log',
        '//aios/storage/indexlib/file_system:FileSystemDefine',
        '//aios/storage/indexlib/util/cache'
    ]
)
