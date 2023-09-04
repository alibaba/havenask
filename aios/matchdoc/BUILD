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
    include_prefix='matchdoc',
    strip_include_prefix='matchdoc',
    visibility=['//visibility:public'],
    deps=['@flatbuffers']
)
cc_library(
    name='matchdoc',
    srcs=glob([
        'matchdoc/FieldGroup.cpp', 'matchdoc/FieldGroupBuilder.cpp',
        'matchdoc/FieldGroupSerdes.cpp', 'matchdoc/MatchDoc.cpp',
        'matchdoc/MatchDocAllocator.cpp', 'matchdoc/MatchDocFormatter.cpp',
        'matchdoc/Reference.cpp', 'matchdoc/ReferenceSet.cpp',
        'matchdoc/ReferenceTypesWrapper.cpp', 'matchdoc/VectorStorage.cpp'
    ]),
    hdrs=glob([
        'matchdoc/CommonDefine.h', 'matchdoc/FieldGroup.h',
        'matchdoc/FieldGroupBuilder.h', 'matchdoc/FieldGroupSerdes.h',
        'matchdoc/MatchDoc.h', 'matchdoc/MatchDocAllocator.h',
        'matchdoc/MatchDocFormatter.h', 'matchdoc/MountInfo.h',
        'matchdoc/Reference.h', 'matchdoc/ReferenceSet.h',
        'matchdoc/ReferenceTypesWrapper.h', 'matchdoc/SubDocAccessor.h',
        'matchdoc/ToString.h', 'matchdoc/Trait.h',
        'matchdoc/TraitFlatbuffers.h', 'matchdoc/ValueType.h',
        'matchdoc/VectorDocStorage.h', 'matchdoc/VectorStorage.h'
    ]),
    copts=['-Werror'],
    include_prefix='matchdoc',
    strip_include_prefix='matchdoc',
    visibility=['//visibility:public'],
    deps=[
        ':matchdoc_fbs_h', '//aios/autil:NoCopyable', '//aios/autil:demangle',
        '//aios/autil:hyper_loglog', '//aios/autil:mem_pool_container',
        '//aios/autil:murmur_hash', '//aios/autil:pack_data',
        '//aios/autil:string_helper', '//aios/autil:time_span'
    ]
)
cc_library(
    name='matchdoc_toolkit',
    srcs=glob(['matchdoc/toolkit/*.cpp']),
    hdrs=glob(['matchdoc/toolkit/*.h']),
    copts=['-Werror'],
    include_prefix='matchdoc',
    strip_include_prefix='matchdoc',
    visibility=['//visibility:public'],
    deps=[':matchdoc', '//aios/autil:json', '//aios/autil:multi_value_helper']
)
