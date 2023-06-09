load('@rules_cc//cc:defs.bzl', 'cc_library')
cc_library(
    name='index_format_option',
    srcs=['index_format_option.cpp'],
    hdrs=['index_format_option.h'],
    copts=['-Werror'],
    include_prefix='indexlib/index/normal/inverted_index/format',
    visibility=['//aios/storage/indexlib/indexlib:__subpackages__'],
    deps=[
        '//aios/storage/indexlib/index/inverted_index/format:IndexFormatOption',
        '//aios/storage/indexlib/indexlib/config'
    ],
    alwayslink=True
)
