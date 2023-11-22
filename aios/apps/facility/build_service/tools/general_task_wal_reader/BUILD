cc_library(
    name='wal_reader_worker',
    srcs=['WalReaderWorker.cpp'],
    hdrs=['WalReaderWorker.h'],
    include_prefix='general_task_wal_reader',
    deps=[
        '//aios/apps/facility/build_service:proto_cc_proto',
        '//aios/autil:string_helper', '//aios/storage/indexlib/file_system/wal',
        '//third_party/readline', '@com_google_absl//absl/flags:flag',
        '@com_google_absl//absl/flags:usage'
    ]
)
cc_binary(
    name='general_task_wal_reader',
    srcs=['WalReader.cpp'],
    visibility=['//visibility:public'],
    deps=[':wal_reader_worker', '@com_google_absl//absl/flags:parse']
)
