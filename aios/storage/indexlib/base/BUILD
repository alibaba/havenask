load('//aios/storage:defs.bzl', 'strict_cc_library')
package(default_visibility=['//aios/storage:__subpackages__'])
strict_cc_library(
    name='progress',
    srcs=[],
    hdrs=[],
    visibility=['//visibility:public'],
    deps=[':Progress']
)
strict_cc_library(
    name='constants',
    srcs=[],
    hdrs=[],
    visibility=['//visibility:public'],
    deps=[':Constant', ':FieldType']
)
strict_cc_library(
    name='Status',
    deps=[
        '//aios/autil:result', '//aios/autil:span', '//aios/autil:string_helper'
    ]
)
strict_cc_library(name='Types', srcs=[])
strict_cc_library(name='Define', srcs=[])
strict_cc_library(name='Constant', srcs=[], deps=[':Types'])
strict_cc_library(
    name='PathUtil',
    deps=[
        ':Constant', ':Types', '//aios/autil:regex',
        '//aios/autil:string_helper'
    ]
)
strict_cc_library(name='MemoryQuotaController', deps=[':Status'])
strict_cc_library(name='BinaryStringUtil', srcs=[])
strict_cc_library(
    name='MemoryQuotaSynchronizer', deps=[':MemoryQuotaController']
)
strict_cc_library(name='NoExceptionWrapper', srcs=[], deps=[':Status'])
strict_cc_library(name='Progress', srcs=[])
strict_cc_library(name='FieldType', srcs=[])
strict_cc_library(
    name='FieldTypeUtil', deps=[':FieldType', '//aios/autil:multi_value']
)
