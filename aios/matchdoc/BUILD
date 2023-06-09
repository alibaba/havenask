load('@flatbuffers//:build_defs.bzl', 'flatbuffer_library_public')
flatbuffer_library_public(
    name='matchdoc_fbs',
    srcs=['matchdoc/flatbuffer/MatchDoc.fbs'],
    outs=['matchdoc/flatbuffer/MatchDoc_generated.h'],
    language_flag='-c'
)
cc_library(
    name='matchdoc_fbs_h',
    hdrs=['matchdoc/flatbuffer/MatchDoc_generated.h'],
    deps=['@flatbuffers//:flatbuffers'],
    include_prefix='matchdoc',
    strip_include_prefix='matchdoc',
    visibility=['//visibility:public']
)
cc_library(
    name='matchdoc',
    srcs=glob([
        'matchdoc/FieldGroup.cpp', 'matchdoc/MatchDoc.cpp',
        'matchdoc/MatchDocAllocator.cpp', 'matchdoc/MatchDocFormatter.cpp',
        'matchdoc/Reference.cpp', 'matchdoc/VectorDocStorage.cpp'
    ]),
    hdrs=glob([
        'matchdoc/CommonDefine.h', 'matchdoc/DocStorage.h',
        'matchdoc/FieldGroup.h', 'matchdoc/MatchDoc.h',
        'matchdoc/MatchDocAllocator.h', 'matchdoc/MatchDocFormatter.h',
        'matchdoc/MountInfo.h', 'matchdoc/Reference.h',
        'matchdoc/SubDocAccessor.h', 'matchdoc/Trait.h',
        'matchdoc/TraitFlatbuffers.h', 'matchdoc/ValueType.h',
        'matchdoc/VectorDocStorage.h'
    ]),
    deps=[
        ':matchdoc_fbs_h', '//aios/autil:hyper_loglog',
        '//aios/autil:pack_data', '//aios/autil:murmur_hash',
        '//aios/autil:mem_pool_container', '//aios/autil:string_helper',
        '//aios/autil:time_span'
    ],
    visibility=['//visibility:public'],
    include_prefix='matchdoc',
    copts=['-Werror'],
    strip_include_prefix='matchdoc'
)
