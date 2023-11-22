load('@flatbuffers//:build_defs.bzl', 'flatbuffer_library_public')
flatbuffer_library_public(
    name='matchdoc_fbs',
    srcs=['flatbuffer/MatchDoc.fbs'],
    outs=['flatbuffer/MatchDoc_generated.h'],
    language_flag='-c'
)
cc_library(
    name='matchdoc_fbs_h',
    hdrs=['flatbuffer/MatchDoc_generated.h'],
    include_prefix='matchdoc',
    visibility=['//visibility:public'],
    deps=['@flatbuffers']
)
cc_library(
    name='matchdoc',
    srcs=glob([
        'FieldGroup.cpp', 'FieldGroupBuilder.cpp', 'FieldGroupSerdes.cpp',
        'MatchDoc.cpp', 'MatchDocAllocator.cpp', 'MatchDocFormatter.cpp',
        'Reference.cpp', 'ReferenceSet.cpp', 'ReferenceTypesWrapper.cpp',
        'VectorStorage.cpp'
    ]),
    hdrs=glob([
        'CommonDefine.h', 'FieldGroup.h', 'FieldGroupBuilder.h',
        'FieldGroupSerdes.h', 'MatchDoc.h', 'MatchDocAllocator.h',
        'MatchDocFormatter.h', 'MountInfo.h', 'Reference.h', 'ReferenceSet.h',
        'ReferenceTypesWrapper.h', 'SubDocAccessor.h', 'ToString.h', 'Trait.h',
        'TraitFlatbuffers.h', 'ValueType.h', 'VectorDocStorage.h',
        'VectorStorage.h'
    ]),
    copts=['-Werror'],
    include_prefix='matchdoc',
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
    srcs=glob(['toolkit/*.cpp']),
    hdrs=glob(['toolkit/*.h']),
    copts=['-Werror'],
    include_prefix='matchdoc',
    visibility=['//visibility:public'],
    deps=[':matchdoc', '//aios/autil:json', '//aios/autil:multi_value_helper']
)
