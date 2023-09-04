cc_library(
    name='wal_reader_worker',
    srcs=['WalReaderWorker.cpp'],
    hdrs=['WalReaderWorker.h'],
    include_prefix='general_task_wal_reader',
    deps=[
        '//aios/apps/facility/build_service:bs_proto',
        '//aios/storage/indexlib/file_system/wal', '//aios/autil:string_helper',
        '//third_party/readline', '@com_google_absl//absl/flags:flag'
    ]
)
cc_binary(
    name='general_task_wal_reader',
    srcs=['WalReader.cpp'],
    deps=[':wal_reader_worker', '@com_google_absl//absl/flags:parse'],
    visibility=['//visibility:public']
)
