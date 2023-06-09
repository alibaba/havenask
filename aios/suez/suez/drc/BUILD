package(default_visibility=['//aios/suez:__subpackages__'])
cc_library(
    name='suez_drc',
    hdrs=[
        'DrcConfig.h', 'LogRecord.h', 'LogRecordBuilder.h', 'LogReader.h',
        'LogWriter.h', 'Source.h', 'SwiftSource.h', 'Sink.h', 'SwiftSink.h',
        'Checkpoint.h', 'LogRewriter.h', 'Update2Add.h', 'BinaryRecord.h',
        'BinaryRecordDecoder.h', 'LogReplicator.h', 'LogReplicationPipeline.h',
        'LogReplicatorCreator.h', 'IgnoredLog.h', 'LogTracer.h'
    ],
    srcs=[
        'DrcConfig.cpp', 'LogRecord.cpp', 'LogRecordBuilder.cpp',
        'LogReader.cpp', 'LogWriter.cpp', 'SwiftSource.cpp', 'SwiftSink.cpp',
        'Checkpoint.cpp', 'Update2Add.cpp', 'BinaryRecord.cpp',
        'BinaryRecordDecoder.cpp', 'LogReplicator.cpp',
        'LogReplicationPipeline.cpp', 'LogReplicatorCreator.cpp',
        'IgnoredLog.cpp', 'LogTracer.cpp'
    ],
    include_prefix='suez/drc',
    deps=[
        '//aios/autil:log', '//aios/autil:span', '//aios/autil:string_helper',
        '//aios/autil:hash_func', '//aios/autil:time',
        '//aios/kmonitor:kmonitor_client_cpp', '@swift//:swift',
        '//aios/worker_framework:worker_framework',
        '//aios/storage/indexlib:tablet',
        '//aios/storage/indexlib/table/kv_table:reader',
        '//aios/suez:suez_inner_sdk',
        '//aios/apps/facility/build_service:bs_config',
        '//aios/apps/facility/build_service:bs_util'
    ],
    alwayslink=True,
    visibility=['//visibility:public']
)
