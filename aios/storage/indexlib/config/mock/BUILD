load('//aios/storage:defs.bzl', 'strict_cc_library')
package(default_visibility=['//aios/storage/indexlib:__subpackages__'])
strict_cc_library(
    name='FakeTabletSchema',
    deps=[
        '//aios/autil:json', '//aios/autil:log',
        '//aios/storage/indexlib/base:Status',
        '//aios/storage/indexlib/base:constants',
        '//aios/storage/indexlib/config:FieldConfig',
        '//aios/storage/indexlib/config:IIndexConfig',
        '//aios/storage/indexlib/config:ITabletSchema',
        '//aios/storage/indexlib/config:MutableJson',
        '//aios/storage/indexlib/util:JsonMap'
    ]
)
