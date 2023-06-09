package(
    default_visibility=[
        '//aios/storage/indexlib/indexlib/partition:__subpackages__'
    ]
)
cc_library(
    name='document-deduper',
    srcs=[
        'building_segment_document_deduper.cpp',
        'built_segments_document_deduper.cpp', 'document_deduper.cpp',
        'document_deduper_creator.cpp',
        'sub_doc_built_segments_document_deduper.cpp'
    ],
    hdrs=[
        'building_segment_document_deduper.h',
        'built_segments_document_deduper.h', 'document_deduper.h',
        'document_deduper_creator.h',
        'sub_doc_building_segment_document_deduper.h',
        'sub_doc_built_segments_document_deduper.h'
    ],
    copts=['-Werror'],
    include_prefix='indexlib/partition/document_deduper',
    visibility=['//aios/storage/indexlib/indexlib:__subpackages__'],
    deps=[
        '//aios/autil:net', '//aios/storage/indexlib/indexlib/index:index-all',
        '//aios/storage/indexlib/indexlib/index_base',
        '//aios/storage/indexlib/indexlib/partition/modifier'
    ],
    alwayslink=True
)
