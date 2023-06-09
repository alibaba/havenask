package(default_visibility=['//aios/storage/indexlib:__subpackages__'])
load('//aios/storage/indexlib:defs.bzl', 'indexlib_cc_library')
indexlib_cc_library(
    name='KeyMap',
    deps=[
        '//aios/autil:hash_map', '//aios/autil:mem_pool_base',
        '//aios/autil:span', '//aios/autil:string_type'
    ]
)
indexlib_cc_library(name='KeyMapManager', deps=[':KeyMap', '//aios/autil:lock'])
indexlib_cc_library(
    name='DefaultRawDocFieldIterator',
    deps=['//aios/storage/indexlib/document:RawDocFieldIterator']
)
indexlib_cc_library(
    name='DefaultRawDocument',
    deps=[
        ':DefaultRawDocFieldIterator', ':KeyMapManager',
        '//aios/autil:string_helper', '//aios/storage/indexlib/base:constants',
        '//aios/storage/indexlib/document:RawDocument',
        '//aios/storage/indexlib/framework:Locator'
    ]
)
