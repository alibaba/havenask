package_group(
    name='indexlib_extensions',
    packages=[
        '//aios/storage/khronos/...', '//aios/storage/rocks/...',
        '//aios/storage/tools/...'
    ]
)
config_setting(
    name='with_storage_extension',
    values={'define': 'storage_extension=khronos'}
)
config_setting(
    name='with_rocks_storage_extension',
    values={'define': 'with_rocks_storage_extension=true'}
)
cc_library(
    name='table_models',
    srcs=[],
    hdrs=[],
    visibility=['//visibility:public'],
    deps=((['//aios/storage/indexlib/table:all'] + []) + [])
)
