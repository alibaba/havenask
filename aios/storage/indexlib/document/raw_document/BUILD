package(default_visibility=['//aios/storage/indexlib:__subpackages__'])
load('//aios/storage:defs.bzl', 'strict_cc_library')
strict_cc_library(
    name='KeyMap',
    deps=[
        '//aios/autil:hash_map', '//aios/autil:mem_pool_base',
        '//aios/autil:span', '//aios/autil:string_type'
    ]
)
strict_cc_library(name='KeyMapManager', deps=[':KeyMap', '//aios/autil:lock'])
strict_cc_library(
    name='DefaultRawDocFieldIterator',
    deps=['//aios/storage/indexlib/document:RawDocFieldIterator']
)
strict_cc_library(
    name='DefaultRawDocument',
    deps=[
        ':DefaultRawDocFieldIterator', ':KeyMapManager',
        '//aios/autil:string_helper', '//aios/storage/indexlib/base:constants',
        '//aios/storage/indexlib/document:RawDocument',
        '//aios/storage/indexlib/framework:Locator'
    ]
)
