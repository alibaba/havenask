cc_library(
    name='util',
    srcs=['build_work_item.cpp', 'patch_work_item.cpp'],
    hdrs=[
        'build_work_item.h', 'index_metrics_base.h', 'patch_work_item.h',
        'segment_base_doc_id_info.h', 'segment_file_compress_util.h',
        'segment_modifier_container.h'
    ],
    copts=['-Werror'],
    include_prefix='indexlib/index/normal/util',
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[
        '//aios/storage/indexlib/config:FileCompressConfigV2',
        '//aios/storage/indexlib/config:SingleFileCompressConfig',
        '//aios/storage/indexlib/indexlib:indexlib_misc',
        '//aios/storage/indexlib/indexlib/config',
        '//aios/storage/indexlib/indexlib/document',
        '//aios/storage/indexlib/indexlib/util:util_all'
    ]
)
