load('//aios/storage:defs.bzl', 'strict_cc_library')
package(
    default_visibility=['//aios/storage/indexlib/file_system:__subpackages__']
)
strict_cc_library(
    name='interface',
    srcs=[],
    hdrs=[
        'DeleteOption.h', 'FenceContext.h', 'FslibFileWrapper.h',
        'FslibOption.h', 'FslibWrapper.h', 'IoConfig.h'
    ],
    visibility=['//visibility:public']
)
strict_cc_library(
    name='fslib',
    srcs=[
        'DataFlushController.cpp', 'ExceptionTrigger.cpp',
        'FslibCommonFileWrapper.cpp', 'FslibFileWrapper.cpp',
        'FslibWrapper.cpp', 'IoConfig.cpp', 'MultiPathDataFlushController.cpp'
    ],
    hdrs=[
        'DataFlushController.h', 'ExceptionTrigger.h',
        'FslibCommonFileWrapper.h', 'MultiPathDataFlushController.h'
    ],
    deps=[
        ':interface', '//aios/autil:lock', '//aios/autil:log',
        '//aios/autil:thread', '//aios/filesystem/fslib:fslib-framework',
        '//aios/storage/indexlib/file_system:ErrorCode',
        '//aios/storage/indexlib/file_system:FSResult',
        '//aios/storage/indexlib/file_system:FileSystemDefine',
        '//aios/storage/indexlib/util:Singleton',
        '//aios/storage/indexlib/util:epochid_util',
        '//aios/storage/indexlib/util:path_util'
    ]
)
