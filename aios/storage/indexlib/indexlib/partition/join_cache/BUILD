cc_library(
    name='base',
    srcs=['join_field.cpp'],
    hdrs=['join_field.h'],
    copts=['-Werror'],
    include_prefix='indexlib/partition/join_cache/',
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[
        '//aios/autil:string_helper',
        '//aios/storage/indexlib/indexlib:indexlib_h'
    ]
)
cc_library(
    name='join_cache_sdk',
    srcs=['join_docid_reader_creator.cpp', 'join_info.cpp'],
    hdrs=[
        'join_docid_reader.h', 'join_docid_reader_base.h',
        'join_docid_reader_creator.h', 'join_info.h'
    ],
    copts=['-Werror'],
    include_prefix='indexlib/partition/join_cache',
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[
        ':base', '//aios/storage/indexlib/indexlib/partition:partition_reader',
        '//aios/storage/indexlib/index/primary_key:reader',
        '//aios/storage/indexlib/indexlib/index/normal/primarykey:reader',
        '//aios/storage/indexlib/indexlib/index/normal/attribute:reader_interface'
    ]
)
cc_library(
    name='join_cache_initializer',
    srcs=['join_cache_initializer.cpp', 'join_cache_initializer_creator.cpp'],
    hdrs=['join_cache_initializer.h', 'join_cache_initializer_creator.h'],
    copts=['-Werror'],
    include_prefix='indexlib/partition/join_cache',
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[
        '//aios/storage/indexlib/indexlib/index/normal/attribute:reader_interface',
        '//aios/storage/indexlib/indexlib/index/normal/primarykey:reader',
        '//aios/storage/indexlib/indexlib/partition:partition_interface'
    ]
)
