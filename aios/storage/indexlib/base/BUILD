load('//aios/storage/indexlib:defs.bzl', 'indexlib_cc_library')
package(default_visibility=['//aios/storage:__subpackages__'])
indexlib_cc_library(
    name='progress',
    srcs=[],
    hdrs=[],
    visibility=['//visibility:public'],
    deps=[':Progress']
)
indexlib_cc_library(
    name='constants',
    srcs=[],
    hdrs=[],
    visibility=['//visibility:public'],
    deps=[':Constant', ':FieldType']
)
indexlib_cc_library(
    name='Status',
    deps=[
        '//aios/autil:result', '//aios/autil:span', '//aios/autil:string_helper'
    ]
)
indexlib_cc_library(name='Types', srcs=[])
indexlib_cc_library(name='Define', srcs=[])
indexlib_cc_library(name='Constant', srcs=[], deps=[':Types'])
indexlib_cc_library(
    name='PathUtil',
    deps=[
        ':Constant', ':Types', '//aios/autil:regex',
        '//aios/autil:string_helper'
    ]
)
indexlib_cc_library(name='MemoryQuotaController', deps=[':Status'])
indexlib_cc_library(name='BinaryStringUtil', srcs=[])
indexlib_cc_library(
    name='MemoryQuotaSynchronizer', deps=[':MemoryQuotaController']
)
indexlib_cc_library(name='NoExceptionWrapper', srcs=[], deps=[':Status'])
indexlib_cc_library(name='Progress', srcs=[])
indexlib_cc_library(name='FieldType', srcs=[])
indexlib_cc_library(
    name='FieldTypeUtil', deps=[':FieldType', '//aios/autil:multi_value']
)
