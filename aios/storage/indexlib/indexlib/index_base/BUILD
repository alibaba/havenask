cc_library(
    name='index_base',
    srcs=glob(['*.cpp', 'index_meta/*.cpp', 'patch/*.cpp', 'segment/*.cpp'],
              exclude=[
                  'branch_fs.cpp', 'common_branch_hinter_option.cpp',
                  'common_branch_hinter.cpp', 'default_branch_hinter.cpp'
              ]),
    hdrs=glob(['*.h', 'index_meta/*.h', 'patch/*.h', 'segment/*.h']),
    copts=['-Werror'],
    include_prefix='indexlib/index_base',
    visibility=['//aios/storage/indexlib/indexlib:indexlibV1'],
    deps=[
        ':branch', '//aios/storage/indexlib/config:TabletOptions',
        '//aios/storage/indexlib/file_system',
        '//aios/storage/indexlib/framework:SegmentDescriptions',
        '//aios/storage/indexlib/framework:SegmentInfo',
        '//aios/storage/indexlib/framework:SegmentMetrics',
        '//aios/storage/indexlib/framework:SegmentStatistics',
        '//aios/storage/indexlib/framework:TabletDeployer',
        '//aios/storage/indexlib/framework:VersionDeployDescription',
        '//aios/storage/indexlib/framework:VersionMeta',
        '//aios/storage/indexlib/framework:tablet_schema_loader',
        '//aios/storage/indexlib/framework/hooks',
        '//aios/storage/indexlib/framework/lifecycle',
        '//aios/storage/indexlib/indexlib:indexlib_h',
        '//aios/storage/indexlib/indexlib:indexlib_misc',
        '//aios/storage/indexlib/indexlib/common',
        '//aios/storage/indexlib/indexlib/config',
        '//aios/storage/indexlib/indexlib/document:raw_document',
        '//aios/storage/indexlib/legacy/index_base:PartitionMeta',
        '//aios/storage/indexlib/legacy/index_base:SchemaAdapter',
        '//aios/storage/indexlib/table/normal_table:DimensionDescription',
        '//aios/storage/indexlib/table/normal_table/index_task:SegmentMapper'
    ],
    alwayslink=True
)
cc_library(
    name='branch',
    srcs=[
        'branch_fs.cpp', 'common_branch_hinter.cpp',
        'common_branch_hinter_option.cpp', 'default_branch_hinter.cpp'
    ],
    hdrs=[
        'branch_fs.h', 'common_branch_hinter.h',
        'common_branch_hinter_option.h', 'default_branch_hinter.h'
    ],
    copts=['-Werror'],
    include_prefix='indexlib/index_base',
    visibility=['//aios/storage/indexlib/indexlib:__subpackages__'],
    deps=[
        '//aios/storage/indexlib/file_system',
        '//aios/storage/indexlib/indexlib:indexlib_h',
        '//aios/storage/indexlib/indexlib:indexlib_misc'
    ],
    alwayslink=True
)
