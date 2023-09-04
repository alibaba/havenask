load('//bazel:defs.bzl', 'cc_proto')
load(
    '@rules_cc//examples:experimental_cc_shared_library.bzl',
    'cc_shared_library'
)
load('//bazel:bundle.bzl', 'bundle_files')
TEST_COMMON_DATA = glob(['testdata/**'])
filegroup(
    name='bs_run_script_task_file',
    srcs=[
        'build_service_tasks/script_task/local_script_runner',
        'build_service_tasks/script_task/run_task_script',
        'build_service_tasks/script_task/task_script_daemon'
    ],
    visibility=['//visibility:public']
)
bundle_files(
    name='bs_script_task_package',
    srcs=[':bs_run_script_task_file'],
    prefix='usr/local/bin',
    strip_prefix='build_service_tasks/script_task',
    tags=['manual'],
    visibility=['//visibility:public']
)
cc_library(
    name='bs_tasks_basic',
    include_prefix='build_service_tasks',
    strip_include_prefix='build_service_tasks',
    deps=[
        '//aios/aios/common/beeper',
        '//aios/apps/facility/build_service:bs_basic',
        '//aios/apps/facility/build_service:bs_build_in_task_factory',
        '//aios/apps/facility/build_service:bs_config',
        '//aios/apps/facility/build_service:bs_task_base',
        '//aios/apps/facility/build_service:bs_util',
        '//aios/filesystem/fslib:fslib-framework',
        '//aios/storage/indexlib/indexlib',
        '//aios/storage/indexlib/indexlib/partition:index_roll_back_util',
        '//aios/worker_framework'
    ]
)
cc_library(
    name='bs_tasks_channel',
    srcs=glob(['build_service_tasks/channel/*.cpp']),
    hdrs=glob(['build_service_tasks/channel/*.h']),
    include_prefix='build_service_tasks',
    strip_include_prefix='build_service_tasks',
    deps=[
        ':bs_tasks_channel_cc_impl', ':bs_tasks_channel_cc_proto_headers',
        '//aios/apps/facility/build_service:bs_util', '//aios/network/arpc',
        '//aios/network/leader_client'
    ]
)
cc_library(
    name='bs_tasks_channel_test_helper',
    srcs=glob(['build_service_tasks/channel/test/*.cpp']),
    hdrs=glob(['build_service_tasks/channel/test/*.h']),
    include_prefix='build_service_tasks',
    strip_include_prefix='build_service_tasks',
    deps=[':bs_tasks_basic', ':bs_tasks_channel']
)
cc_library(
    name='bs_tasks_extract_doc',
    srcs=glob(['build_service_tasks/extract_doc/*.cpp']),
    hdrs=glob([
        'build_service_tasks/extract_doc/*.h',
        'build_service_tasks/factory/BuildServiceTaskFactory.h'
    ]),
    include_prefix='build_service_tasks',
    strip_include_prefix='build_service_tasks',
    deps=[':bs_tasks_basic']
)
cc_proto(
    name='bs_tasks_channel',
    srcs=glob(['build_service_tasks/channel/*.proto']),
    import_prefix='build_service_tasks',
    strip_import_prefix='build_service_tasks',
    deps=['//aios/network/arpc:proto']
)
cc_library(
    name='bs_tasks_clone_index',
    srcs=glob(['build_service_tasks/cloneIndex/*.cpp']),
    hdrs=glob(['build_service_tasks/cloneIndex/*.h']),
    include_prefix='build_service_tasks',
    strip_include_prefix='build_service_tasks',
    deps=[':bs_tasks_basic', ':bs_tasks_channel']
)
cc_library(
    name='bs_tasks_sync_index',
    srcs=glob(['build_service_tasks/syncIndex/*.cpp']),
    hdrs=glob(['build_service_tasks/syncIndex/*.h']),
    include_prefix='build_service_tasks',
    strip_include_prefix='build_service_tasks',
    deps=[':bs_tasks_basic', ':bs_tasks_channel']
)
cc_library(
    name='bs_tasks_endbuild',
    srcs=glob(['build_service_tasks/endbuild/*.cpp']),
    hdrs=glob(['build_service_tasks/endbuild/*.h']),
    include_prefix='build_service_tasks',
    strip_include_prefix='build_service_tasks',
    deps=[':bs_tasks_basic']
)
cc_library(
    name='bs_tasks_docreclaim',
    srcs=glob(['build_service_tasks/doc_reclaim/*.cpp']),
    hdrs=glob(['build_service_tasks/doc_reclaim/*.h']),
    include_prefix='build_service_tasks',
    strip_include_prefix='build_service_tasks',
    deps=[':bs_tasks_basic']
)
cc_library(
    name='bs_tasks_io',
    srcs=glob(['build_service_tasks/io/*.cpp']),
    hdrs=glob(['build_service_tasks/io/*.h']),
    include_prefix='build_service_tasks',
    strip_include_prefix='build_service_tasks',
    deps=[':bs_tasks_basic']
)
cc_library(
    name='bs_tasks_prepare_data_source',
    srcs=glob(['build_service_tasks/prepare_data_source/*.cpp']),
    hdrs=glob(['build_service_tasks/prepare_data_source/*.h']),
    include_prefix='build_service_tasks',
    strip_include_prefix='build_service_tasks',
    deps=[':bs_tasks_basic']
)
cc_library(
    name='bs_tasks_repartition',
    srcs=glob(['build_service_tasks/repartition/*.cpp']),
    hdrs=glob(['build_service_tasks/repartition/*.h']),
    include_prefix='build_service_tasks',
    strip_include_prefix='build_service_tasks',
    deps=[':bs_tasks_basic']
)
cc_library(
    name='bs_tasks_rollback',
    srcs=glob(['build_service_tasks/rollback/*.cpp']),
    hdrs=glob(['build_service_tasks/rollback/*.h']),
    include_prefix='build_service_tasks',
    strip_include_prefix='build_service_tasks',
    deps=[':bs_tasks_basic']
)
cc_library(
    name='bs_tasks_batch_control',
    srcs=glob(['build_service_tasks/batch_control/*.cpp']),
    hdrs=glob(['build_service_tasks/batch_control/*.h']),
    include_prefix='build_service_tasks',
    strip_include_prefix='build_service_tasks',
    deps=[':bs_tasks_basic']
)
cc_library(
    name='bs_tasks_script_task',
    srcs=glob(['build_service_tasks/script_task/*.cpp'],
              exclude=['build_service_tasks/script_task/LocalScriptRunner.cpp'
                      ]),
    hdrs=glob(['build_service_tasks/script_task/*.h']),
    include_prefix='build_service_tasks',
    strip_include_prefix='build_service_tasks',
    deps=[':bs_tasks_basic']
)
genrule(
    name='cp_run_task_script',
    srcs=[':run_task_script'],
    outs=['testdata/config_task_scripts/task_scripts/bin/run_task_script'],
    cmd='cp $(locations :run_task_script) $@',
    visibility=['//visibility:public']
)
genrule(
    name='cp_task_script_daemon',
    srcs=[':task_script_daemon'],
    outs=['testdata/config_task_scripts/task_scripts/bin/task_script_daemon'],
    cmd='cp $(locations :task_script_daemon) $@',
    visibility=['//visibility:public']
)
cc_library(
    name='bs_tasks_testbase',
    srcs=[],
    hdrs=glob(['build_service_tasks/test/unittest.h']),
    include_prefix='build_service_tasks',
    strip_include_prefix='build_service_tasks',
    deps=['//aios/unittest_framework']
)
cc_library(
    name='batch_controller_test_helper',
    srcs=['build_service_tasks/batch_control/test/FakeZkWrapper.cpp'],
    hdrs=glob([
        'build_service_tasks/batch_control/test/MockBatchControlWorker.h'
    ]),
    include_prefix='build_service_tasks',
    strip_include_prefix='build_service_tasks',
    deps=[
        '//aios/apps/facility/build_service:bs_util',
        '//aios/apps/facility/cm2/cm_basic', '//aios/worker_framework'
    ]
)
cc_test(
    name='bs_tasks_batch_control_test',
    srcs=glob(['build_service_tasks/batch_control/test/*.cpp']),
    copts=['-fno-access-control'],
    data=TEST_COMMON_DATA,
    deps=[
        ':batch_controller_test_helper', ':bs_tasks_batch_control',
        ':bs_tasks_testbase'
    ]
)
cc_library(
    name='clone_index_test_helper',
    srcs=[],
    hdrs=glob(['build_service_tasks/cloneIndex/test/*.h']),
    include_prefix='build_service_tasks',
    strip_include_prefix='build_service_tasks',
    deps=[
        ':bs_tasks_channel_test_helper', ':bs_tasks_clone_index',
        '//aios/apps/facility/build_service:bs_util'
    ]
)
cc_library(
    name='sync_index_test_helper',
    srcs=[],
    hdrs=glob(['build_service_tasks/cloneIndex/test/*.h']),
    include_prefix='build_service_tasks',
    strip_include_prefix='build_service_tasks',
    deps=[
        ':bs_tasks_channel_test_helper', ':bs_tasks_sync_index',
        '//aios/apps/facility/build_service:bs_util'
    ]
)
cc_test(
    name='bs_tasks_clone_index_test',
    srcs=glob(['build_service_tasks/cloneIndex/test/*.cpp']),
    copts=['-fno-access-control'],
    data=TEST_COMMON_DATA,
    shard_count=1,
    deps=[
        ':bs_tasks_testbase', ':clone_index_test_helper',
        '//aios/apps/facility/cm2/cm_basic/util:fake_zk_wrapper',
        '//aios/filesystem/fslib:fslib-framework'
    ]
)
cc_test(
    name='bs_tasks_sync_index_test',
    srcs=glob(['build_service_tasks/syncIndex/test/*.cpp']),
    copts=['-fno-access-control'],
    data=TEST_COMMON_DATA,
    shard_count=2,
    deps=[':bs_tasks_testbase', ':sync_index_test_helper']
)
cc_library(
    name='extract_doc_test_helper',
    srcs=['build_service_tasks/extract_doc/test/FakeSwiftOutput.cpp'],
    hdrs=glob(['build_service_tasks/extract_doc/test/*.h']),
    include_prefix='build_service_tasks',
    strip_include_prefix='build_service_tasks',
    deps=[
        ':bs_tasks_extract_doc', ':bs_tasks_factory',
        '//aios/apps/facility/build_service:bs_util',
        '//aios/apps/facility/swift:swift_client'
    ]
)
cc_test(
    name='bs_tasks_extract_doc_test',
    srcs=glob(['build_service_tasks/extract_doc/test/ExtractDocTaskTest.cpp']),
    copts=['-fno-access-control'],
    data=TEST_COMMON_DATA,
    shard_count=2,
    deps=[
        ':bs_tasks_testbase', ':extract_doc_test_helper',
        '//aios/storage/indexlib/indexlib:indexlib_testlib'
    ]
)
cc_test(
    name='bs_tasks_rollback_test',
    srcs=glob(['build_service_tasks/rollback/test/*.cpp']),
    copts=['-fno-access-control'],
    data=TEST_COMMON_DATA,
    deps=[
        ':bs_tasks_rollback', ':bs_tasks_testbase',
        '//aios/storage/indexlib/indexlib:indexlib_testlib',
        '//aios/storage/indexlib/indexlib/test:index_meta_maker'
    ]
)
cc_library(
    name='repartition_test_helper',
    srcs=['build_service_tasks/repartition/test/FakeRepartitionTask.cpp'],
    hdrs=glob(['build_service_tasks/repartition/test/FakeRepartitionTask.h']),
    include_prefix='build_service_tasks',
    strip_include_prefix='build_service_tasks',
    deps=[
        ':bs_tasks_repartition', ':bs_tasks_testbase',
        '//aios/storage/indexlib/indexlib'
    ]
)
cc_test(
    name='bs_tasks_repartition_test',
    srcs=glob(['build_service_tasks/repartition/test/RepartitionTaskTest.cpp']),
    copts=['-fno-access-control'],
    data=TEST_COMMON_DATA,
    flaky=True,
    shard_count=2,
    deps=[
        ':bs_tasks_repartition', ':bs_tasks_testbase',
        ':repartition_test_helper',
        '//aios/storage/indexlib/indexlib:indexlib_testlib'
    ]
)
cc_test(
    name='bs_tasks_endbuild_test',
    srcs=glob(['build_service_tasks/endbuild/test/*.cpp']),
    copts=['-fno-access-control'],
    data=TEST_COMMON_DATA,
    deps=[
        ':bs_tasks_endbuild', ':bs_tasks_testbase',
        '//aios/storage/indexlib/indexlib:indexlib_testlib',
        '//aios/storage/indexlib/indexlib/test:index_meta_maker',
        '//aios/storage/indexlib/indexlib/test:partition_state_machine'
    ]
)
cc_test(
    name='bs_tasks_io_test',
    srcs=glob(['build_service_tasks/io/test/*.cpp']),
    copts=['-fno-access-control'],
    data=TEST_COMMON_DATA,
    deps=([':bs_tasks_io', ':bs_tasks_testbase'] +
          ['//aios/future_lite/future_lite/executors:simple_async_io_executor'])
)
cc_library(
    name='bs_tasks_factory',
    srcs=glob(['build_service_tasks/factory/*.cpp']),
    hdrs=glob(['build_service_tasks/factory/*.h']),
    include_prefix='build_service_tasks',
    strip_include_prefix='build_service_tasks',
    deps=[
        ':bs_tasks_endbuild', ':bs_tasks_docreclaim', ':bs_tasks_io',
        ':bs_tasks_prepare_data_source', ':bs_tasks_repartition',
        ':bs_tasks_rollback', ':bs_tasks_batch_control',
        ':bs_tasks_script_task', ':bs_tasks_extract_doc',
        ':bs_tasks_clone_index', ':bs_tasks_sync_index'
    ]
)
cc_shared_library(
    name='build_service_tasks',
    preloaded_deps=[
        ':bs_tasks_channel_cc_proto', ':bs_tasks_channel_cc_proto_headers',
        ':bs_tasks_basic', '//aios/autil:string_type',
        '//aios/autil:daily_run_mode', '//aios/autil:env_util',
        '//aios/autil:thread', '//aios/autil:lock_free',
        '//aios/autil:string_helper', '//aios/autil:log', '//aios/autil:lock',
        '//aios/apps/facility/cm2/cm_basic:cm_basic',
        '//aios/apps/facility/build_service:bs_util',
        '//aios/storage/indexlib/indexlib:indexlib',
        '//aios/filesystem/fslib:fslib-framework',
        '//aios/resource_reader:resource_reader',
        '//aios/network/arpc:proto_cc_impl',
        '//aios/network/arpc:proto_cc_proto',
        '//aios/network/rdma:arpc_metric_api',
        '//aios/network/arpc/arpc/metric:arpc_metric',
        '//aios/network/anet:anet', '//aios/alog:alog',
        '//aios/kmonitor:kmonitor_client_cpp', '@rapidjson//:rapidjson',
        '@com_google_protobuf//:protobuf',
        '@com_google_protobuf//:cc_wkt_protos'
    ],
    roots=['bs_tasks_factory'],
    static_deps=[
        '//aios/resource_reader:resource_reader',
        '//aios/network/leader_client:leader_client',
        '//aios/network/arpc:arpc', '//aios/autil:NoCopyable',
        '//aios/autil:murmur_hash', '//aios/autil:time',
        '//aios/worker_framework:leader_locator',
        '//aios/worker_framework:leader_elector', ':bs_tasks_factory',
        ':bs_tasks_endbuild', ':bs_tasks_docreclaim', ':bs_tasks_io',
        ':bs_tasks_prepare_data_source', ':bs_tasks_repartition',
        ':bs_tasks_rollback', ':bs_tasks_batch_control',
        ':bs_tasks_script_task', ':bs_tasks_extract_doc',
        ':bs_tasks_clone_index', ':bs_tasks_sync_index',
        ':bs_tasks_channel_cc_impl', 'bs_tasks_channel'
    ],
    visibility=['//visibility:public']
)
