cc_library(
    name='multi_region_rehasher',
    hdrs=['multi_region_rehasher.h'],
    copts=['-Werror'],
    include_prefix='indexlib/common',
    visibility=['//aios/storage/indexlib:__subpackages__']
)
cc_library(
    name='field_format',
    srcs=glob([
        'field_format/attribute/*.cpp', 'field_format/date/*.cpp',
        'field_format/range/*.cpp', 'field_format/pack_attribute/*.cpp',
        'field_format/section_attribute/*.cpp', 'field_format/spatial/*.cpp',
        'field_format/spatial/shape/*.cpp',
        'field_format/spatial/geo_hash/geo_hash_util.cpp',
        'field_format/customized_index/*.cpp'
    ]),
    hdrs=glob([
        'field_format/attribute/*.h', 'field_format/date/*.h',
        'field_format/range/*.h', 'field_format/pack_attribute/*.h',
        'field_format/section_attribute/*.h', 'field_format/spatial/*.h',
        'field_format/spatial/geo_hash/*.h', 'field_format/spatial/shape/*.h',
        'field_format/customized_index/*.h'
    ]),
    copts=['-Werror', '-mavx'],
    include_prefix='indexlib/common',
    visibility=['//visibility:public'],
    deps=[
        '//aios/autil:memory', '//aios/autil:pack_data',
        '//aios/storage/indexlib/file_system:byte_slice_rw',
        '//aios/storage/indexlib/file_system/stream:file_stream',
        '//aios/storage/indexlib/index/common:NumberTerm',
        '//aios/storage/indexlib/index/common:error_code',
        '//aios/storage/indexlib/index/common/field_format/date:DateTerm',
        '//aios/storage/indexlib/index/common/field_format/spatial:geo_hash',
        '//aios/storage/indexlib/index/common/field_format/spatial:shape',
        '//aios/storage/indexlib/index/common/numeric_compress:EncoderProvider',
        '//aios/storage/indexlib/index/common/numeric_compress:EquivalentCompressReader',
        '//aios/storage/indexlib/index/common/numeric_compress:EquivalentCompressSessionReader',
        '//aios/storage/indexlib/index/common/numeric_compress:EquivalentCompressWriter',
        '//aios/storage/indexlib/index/common/numeric_compress:S16Compressor',
        '//aios/storage/indexlib/indexlib:indexlib_h',
        '//aios/storage/indexlib/indexlib:indexlib_misc',
        '//aios/storage/indexlib/indexlib/config',
        '//aios/storage/indexlib/indexlib/util:util_all',
        '//aios/storage/indexlib/util:status_to_exception'
    ],
    alwayslink=True
)
cc_library(
    name='common',
    srcs=glob(['*.cpp', 'hash_table/*.cpp', 'block_array/*.cpp',
               'chunk/*.cpp']),
    hdrs=glob(['*.h', 'hash_table/*.h', 'block_array/*.h', 'chunk/*.h'],
              exclude=['multi_region_rehasher.h',
                       'hash_table/spatial_value.h']),
    copts=['-Werror', '-mavx'],
    include_prefix='indexlib/common',
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[
        '//aios/storage/indexlib/file_system:byte_slice_rw',
        '//aios/storage/indexlib/index/common:radix_tree',
        '//aios/storage/indexlib/index/common:typed_slice_list',
        '//aios/storage/indexlib/indexlib:indexlib_h',
        '//aios/storage/indexlib/indexlib:indexlib_misc',
        '//aios/storage/indexlib/indexlib/config',
        '//aios/storage/indexlib/indexlib/util:util_all'
    ],
    alwayslink=True
)
filegroup(
    name='common_headers',
    srcs=glob([
        '*.h', 'hash_table/*.h', 'block_array/*.h', 'chunk/*.h',
        'field_format/attribute/*.h', 'field_format/date/*.h',
        'field_format/range/*.h', 'field_format/pack_attribute/*.h',
        'field_format/section_attribute/*.h', 'field_format/spatial/*.h',
        'field_format/spatial/geo_hash/*.h', 'field_format/spatial/shape/*.h',
        'field_format/customized_index/*.h'
    ]),
    visibility=['//aios/storage/indexlib:__subpackages__']
)
