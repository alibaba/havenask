load('@rules_cc//cc:defs.bzl', 'cc_binary', 'cc_library', 'cc_test')
load('//bazel:defs.bzl', 'cc_proto', 'gen_conf', 'gen_conf_lib', 'genlex')
load('//aios/apps/facility/build_service:defs.bzl', 'bs_release_version')
load('//bazel:bundle.bzl', 'bundle_files')
package(
    default_visibility=['//aios/apps/facility/build_service:__subpackages__']
)
gen_conf(
    name='bs_lua_conf',
    srcs=['misc/lua.conf.tpl'],
    outs=['lua.conf'],
    args={'@BS_LUA_SCRIPT_ID': bs_release_version}
)
filegroup(
    name='bs_lua_conf_file',
    srcs=[':bs_lua_conf'],
    visibility=['//visibility:public']
)
filegroup(
    name='do_test_testdata',
    srcs=glob(['testdata/**']),
    visibility=['//visibility:public']
)
filegroup(
    name='do_test_lua_scripts',
    srcs=(
        glob(['build_service/admin/controlflow/test/lua_scripts/**']) + [
            'build_service/admin/controlflow/test/lua_zip.conf',
            'build_service/admin/controlflow/test/lua.zip',
            'build_service/admin/controlflow/test/lua_ns_zip.conf',
            'build_service/admin/controlflow/test/lua_ns.zip',
            'build_service/admin/controlflow/test/lua_ns_first_zip.conf',
            'build_service/admin/controlflow/test/lua_ns_first.zip'
        ]
    ),
    visibility=['//visibility:public']
)
genlex(name='lexer', src='build_service/analyzer/SingleWSScanner.ll')
cc_proto(
    name='proto',
    srcs=glob(['build_service/proto/*.proto']),
    import_prefix='build_service/proto',
    strip_import_prefix='build_service/proto',
    visibility=['//visibility:public'],
    deps=['//aios/hippo:proto', '//aios/network/arpc:proto']
)
filegroup(
    name='install_etcs',
    srcs=glob(['misc/*']),
    visibility=['//visibility:public']
)
cc_library(
    name='bs_basic',
    srcs=[
        'build_service/common/Locator.cpp', 'build_service/common_define.cpp',
        'build_service/config/CLIOptionNames.cpp'
    ],
    hdrs=[
        'build_service/common/BeeperCollectorDefine.h',
        'build_service/common/Locator.h', 'build_service/common_define.h',
        'build_service/config/CLIOptionNames.h',
        'build_service/util/ErrorLogCollector.h', 'build_service/util/Log.h'
    ],
    include_prefix='build_service',
    strip_include_prefix='build_service',
    visibility=['//visibility:public'],
    deps=[
        '//aios/alog', '//aios/network/arpc',
        '//aios/storage/indexlib/indexlib/document:raw_document'
    ],
    alwayslink=True
)
gen_conf_lib(
    name='test_h',
    srcs=['build_service/test/test.h.in'],
    outs=['build_service/test/test.h'],
    args={'@TOP_SRCDIR@': 'aios/apps/facility/build_service'},
    include_prefix='build_service'
)
cc_library(
    name='bs_testbase',
    srcs=[],
    hdrs=['build_service/test/unittest.h'],
    include_prefix='build_service',
    strip_include_prefix='build_service',
    visibility=['//visibility:public'],
    deps=[':bs_util', ':test_h', '//aios/alog', '//aios/unittest_framework']
)
cc_library(
    name='bs_task_target',
    srcs=glob(['build_service/config/TaskTarget.cpp']),
    hdrs=glob(['build_service/config/TaskTarget.h']),
    include_prefix='build_service',
    strip_include_prefix='build_service',
    deps=[':bs_basic'],
    alwayslink=True
)
cc_library(
    name='bs_proto',
    srcs=glob(['build_service/proto/*.cpp']),
    hdrs=glob(['build_service/proto/*.h']),
    include_prefix='build_service',
    strip_include_prefix='build_service',
    deps=[
        ':bs_basic', ':bs_task_target', ':proto_cc_proto',
        '//aios/autil:base64', '//aios/autil:range_util',
        '//aios/network/http_arpc'
    ],
    alwayslink=True
)
cc_library(
    name='bs_proto_test_helper',
    srcs=[
        'build_service/proto/test/ProtoCreator.cpp',
        'build_service/proto/test/ProtoTestHelper.cpp'
    ],
    hdrs=glob(['build_service/proto/test/*.h']),
    include_prefix='build_service',
    strip_include_prefix='build_service',
    deps=[':bs_basic', ':bs_util', ':proto_cc_proto']
)
cc_proto(
    name='proto_test',
    srcs=glob(['build_service/proto/test/*.proto']),
    import_prefix='build_service/proto',
    strip_import_prefix='build_service/proto',
    visibility=['//visibility:public'],
    deps=['//aios/hippo:proto', '//aios/network/arpc:proto']
)
cc_test(
    name='bs_proto_test',
    srcs=glob(['build_service/proto/test/*.cpp']),
    copts=['-fno-access-control'],
    deps=[
        ':bs_basic', ':bs_proto_test_helper', ':bs_testbase',
        ':proto_test_cc_proto'
    ]
)
cc_test(
    name='bs_controlflow_test',
    srcs=glob(['build_service/admin/controlflow/test/*Test.cpp']),
    copts=['-fno-access-control'],
    data=([
        '//aios/apps/facility/build_service/build_service/admin/controlflow/lua:basic_lua',
        '//aios/apps/facility/build_service/build_service/admin/controlflow/lua:batch_build_lua',
        '//aios/apps/facility/build_service/build_service/admin/controlflow/lua:batch_build_v2_lua',
        ':do_test_lua_scripts', ':do_test_testdata'
    ] + ['build_service/admin/controlflow/lua.conf']),
    deps=[':bs_admin', ':bs_testbase', '//third_party/lua']
)
cc_test(
    name='bs_admin_taskcontroller_test',
    srcs=glob(['build_service/admin/taskcontroller/test/*Test.cpp']),
    copts=['-fno-access-control'],
    data=[':do_test_testdata'],
    deps=['bs_proto_test_helper', 'bs_testbase', ':bs_admin']
)
cc_test(
    name='bs_import_task_test',
    srcs=['build_service/admin/test/ImportGenerationInteTest.cpp'],
    copts=['-fno-access-control'],
    data=([
        '//aios/apps/facility/build_service/build_service/admin/controlflow/lua:basic_lua',
        '//aios/apps/facility/build_service/build_service/admin/controlflow/lua:batch_build_lua',
        '//aios/apps/facility/build_service/build_service/admin/controlflow/lua:batch_build_v2_lua',
        '//aios/apps/facility/build_service/build_service/admin/controlflow/lua:update2add_lua',
        '//aios/apps/facility/build_service/build_service/admin/controlflow/lua:high_quality_update2add_lua',
        ':do_test_testdata'
    ] + ['build_service/admin/controlflow/lua.conf']),
    shard_count=1,
    deps=[
        ':bs_admin', ':bs_admin_test_helper', ':bs_common_test_helper',
        ':bs_proto_test_helper', ':bs_testbase', ':bs_util_test_helper',
        ':bs_worker_test_helper', ':fake_file_system', ':fake_redis_client',
        '//aios/apps/facility/cm2/cm_basic/util:fake_zk_wrapper',
        '//third_party/lua'
    ]
)
cc_library(
    name='bs_util',
    srcs=glob(['build_service/util/*.cpp']),
    hdrs=glob(['build_service/util/*.h']),
    include_prefix='build_service',
    strip_include_prefix='build_service',
    visibility=['//visibility:public'],
    deps=[
        ':bs_basic', ':bs_proto',
        '//aios/apps/facility/build_service/build_service/third_party/minizip',
        '//aios/apps/facility/swift:swift_client', '//aios/autil:string_helper',
        '//aios/storage/indexlib/framework:id_generator',
        '//third_party/hiredis'
    ],
    alwayslink=True
)
cc_library(
    name='bs_util_test_helper',
    hdrs=[
        'build_service/util/test/FakeSwiftClient.h',
        'build_service/util/test/FakeSwiftClientCreator.h',
        'build_service/util/test/FakeSwiftWriter.h'
    ],
    include_prefix='build_service',
    strip_include_prefix='build_service',
    deps=[':bs_testbase', ':bs_util']
)
cc_library(
    name='fake_file_system',
    srcs=['build_service/util/test/FakeFileSystem.cpp'],
    hdrs=['build_service/util/test/FakeFileSystem.h'],
    include_prefix='build_service',
    strip_include_prefix='build_service',
    deps=[':bs_testbase', ':bs_util']
)
cc_library(
    name='fake_redis_client',
    srcs=['build_service/util/test/FakeRedisClient.cpp'],
    hdrs=['build_service/util/test/FakeRedisClient.h'],
    include_prefix='build_service',
    strip_include_prefix='build_service',
    deps=[':bs_testbase', ':bs_util']
)
cc_library(
    name='fake_swift_client',
    srcs=[
        'build_service/util/test/FakeSwiftClient.cpp',
        'build_service/util/test/FakeSwiftClientCreator.cpp',
        'build_service/util/test/FakeSwiftWriter.cpp'
    ],
    hdrs=[
        'build_service/util/test/FakeSwiftClient.h',
        'build_service/util/test/FakeSwiftClientCreator.h',
        'build_service/util/test/FakeSwiftWriter.h'
    ],
    include_prefix='build_service',
    strip_include_prefix='build_service',
    deps=[':bs_util']
)
cc_test(
    name='range-test',
    srcs=['build_service/util/test/RangeUtilTest.cpp'],
    copts=['-fno-access-control'],
    data=glob(['testdata/**']),
    deps=[
        ':bs_proto_test_helper', ':bs_testbase', ':bs_util',
        ':bs_util_test_helper', ':fake_file_system', ':fake_redis_client'
    ]
)
cc_test(
    name='bs_util_test',
    srcs=glob(['build_service/util/test/*.cpp']),
    copts=['-fno-access-control'],
    data=glob(['testdata/**']),
    deps=[
        ':bs_proto_test_helper', ':bs_testbase', ':bs_util',
        ':bs_util_test_helper', ':fake_file_system', ':fake_redis_client',
        '//aios/apps/facility/swift/testlib:mock_swift_client',
        '//aios/storage/indexlib:alter_table',
        '//aios/storage/indexlib/framework:tablet_schema_loader',
        '//aios/storage/indexlib/table/normal_table:table'
    ]
)
cc_library(
    name='bs_io',
    srcs=glob(['build_service/io/*.cpp'],
              exclude=['build_service/io/MysqlOutput.cpp']),
    hdrs=glob(['build_service/io/*.h'],
              exclude=['build_service/io/MysqlOutput.h']),
    copts=[],
    include_prefix='build_service',
    strip_include_prefix='build_service',
    visibility=['//visibility:public'],
    deps=[
        ':bs_config',
        '//aios/storage/indexlib/indexlib/partition:indexlib_partition'
    ],
    alwayslink=True
)
cc_test(
    name='bs_io_test',
    srcs=glob(['build_service/io/test/*.cpp']),
    copts=['-fno-access-control'],
    data=glob(['testdata/**']),
    deps=[':bs_io', ':bs_testbase']
)
cc_library(
    name='bs_config',
    srcs=glob(['build_service/config/*.cpp'],
              exclude=[
                  'build_service/config/TaskTarget.cpp',
                  'build_service/config/HashMode.cpp',
                  'build_service/config/CLIOptionNames.cpp'
              ]),
    hdrs=glob(['build_service/config/*.h', 'build_service/plugin/ModuleInfo.h'],
              exclude=['build_service/config/TaskTarget.h']),
    include_prefix='build_service',
    strip_include_prefix='build_service',
    visibility=['//visibility:public'],
    deps=[
        ':bs_hashmode', ':bs_proto', ':bs_task_target', ':bs_util',
        '//aios/filesystem/fslib:fslib-framework',
        '//aios/storage/indexlib/config:options',
        '//aios/storage/indexlib/config:schema',
        '//aios/storage/indexlib/framework:tablet_factory',
        '//aios/storage/indexlib/framework:tablet_schema_loader',
        '//aios/storage/indexlib/indexlib/config',
        '//aios/storage/indexlib/legacy/config:module_info'
    ],
    alwayslink=True
)
cc_library(
    name='bs_hashmode',
    srcs=['build_service/config/HashMode.cpp'],
    hdrs=['build_service/config/HashMode.h'],
    include_prefix='build_service',
    strip_include_prefix='build_service',
    visibility=['//visibility:public'],
    deps=['//aios/autil:json', '//aios/autil:log']
)
cc_test(
    name='bs_config_test',
    srcs=glob(['build_service/config/test/*.cpp'],
              exclude=[
                  'build_service/config/test/FakeRealtimeSchemaListKeeper.cpp'
              ]),
    copts=['-fno-access-control'],
    data=glob(['testdata/**']),
    deps=[
        ':bs_config', ':bs_proto', ':bs_proto_test_helper', ':bs_testbase',
        '//aios/storage/indexlib/table/normal_table:table'
    ]
)
cc_library(
    name='bs_plugin',
    srcs=glob(['build_service/plugin/*.cpp'],
              exclude=['build_service/plugin/ModuleInfo.cpp']),
    hdrs=glob(['build_service/plugin/*.h']),
    include_prefix='build_service',
    strip_include_prefix='build_service',
    visibility=['//visibility:public'],
    deps=[
        ':bs_config', '//aios/resource_reader',
        '//aios/storage/indexlib/indexlib:indexlib_plugin'
    ],
    alwayslink=True
)
cc_library(
    name='bs_common',
    srcs=glob(['build_service/common/*.cpp'],
              exclude=['build_service/common/Locator.cpp']),
    hdrs=glob(['build_service/common/*.h']),
    include_prefix='build_service',
    strip_include_prefix='build_service',
    visibility=[
        '//aios/apps/facility/build_service:__subpackages__',
        '//aios/plugins/havenask_plugins/bs_reader_plugins:__subpackages__'
    ],
    deps=[
        ':bs_config', ':bs_proto', '//aios/autil:resource_container',
        '//aios/storage/indexlib/framework/index_task:task_execute',
        '//aios/storage/indexlib/indexlib',
        '//aios/storage/indexlib/table/normal_table/index_task:document_reclaim',
        '//aios/worker_framework'
    ],
    alwayslink=True
)
cc_library(
    name='bs_common_test_helper',
    srcs=glob([
        'build_service/common/test/Fake*.cpp',
        'build_service/common/test/Mock*.cpp'
    ]),
    hdrs=glob(['build_service/common/test/*.h']),
    include_prefix='build_service',
    strip_include_prefix='build_service',
    deps=[':bs_common', ':bs_testbase']
)
cc_test(
    name='bs_common_test',
    srcs=glob(['build_service/common/test/*.cpp']),
    copts=['-fno-access-control'],
    data=glob(['testdata/**']),
    shard_count=10,
    deps=[
        ':bs_common', ':bs_common_test_helper', ':bs_proto_test_helper',
        ':bs_testbase', ':bs_util_test_helper', ':fake_redis_client',
        ':fake_swift_client',
        '//aios/apps/facility/swift/testlib:mock_swift_client',
        '//aios/storage/indexlib/table/normal_table:table'
    ]
)
cc_library(
    name='bs_analyzer_base',
    srcs=[],
    hdrs=[
        'build_service/analyzer/Token.h', 'build_service/analyzer/Tokenizer.h'
    ],
    copts=['-Wno-register', '-Werror'],
    include_prefix='build_service',
    strip_include_prefix='build_service',
    visibility=['//visibility:public'],
    deps=[
        ':bs_config', ':bs_plugin', '//aios/autil:codec',
        '//aios/storage/indexlib/analyzer',
        '//aios/storage/indexlib/framework/index_task:task_execute',
        '//aios/storage/indexlib/indexlib/document', '//third_party/mxml'
    ],
    alwayslink=True
)
cc_library(
    name='bs_analyzer',
    srcs=((
        glob(['build_service/analyzer/*.cpp'],
             exclude=['build_service/analyzer/BuildInTokenizerFactory.cpp']) +
        [':lexer']
    ) + ['build_service/analyzer/opensource/BuildInTokenizerFactory.cpp']),
    hdrs=glob(['build_service/analyzer/*.h'],
              exclude=[
                  'build_service/analyzer/Token.h',
                  'build_service/analyzer/Tokenizer.h'
              ]),
    copts=['-Wno-register', '-Werror'],
    include_prefix='build_service',
    strip_include_prefix='build_service',
    visibility=['//visibility:public'],
    deps=([
        ':bs_config', ':bs_plugin', ':bs_analyzer_base', '//aios/autil:codec',
        '//aios/storage/indexlib/analyzer',
        '//aios/storage/indexlib/framework/index_task:task_execute',
        '//aios/storage/indexlib/indexlib/document', '//third_party/mxml'
    ] + []),
    alwayslink=True
)
cc_test(
    name='bs_analyzer_test',
    srcs=glob(['build_service/analyzer/test/*.cpp']),
    copts=['-fno-access-control'],
    data=glob(['testdata/**']),
    deps=[':bs_analyzer', ':bs_testbase']
)
cc_library(
    name='bs_document',
    srcs=glob(['build_service/document/*.cpp']),
    hdrs=glob(['build_service/document/*.h']),
    include_prefix='build_service',
    strip_include_prefix='build_service',
    visibility=['//visibility:public'],
    deps=[
        ':bs_config', '//aios/storage/indexlib/document',
        '//aios/storage/indexlib/document/normal:extend_document',
        '//aios/storage/indexlib/indexlib/document'
    ],
    alwayslink=True
)
cc_library(
    name='bs_document_test_helper',
    srcs=['build_service/document/test/DocumentTestHelper.cpp'],
    hdrs=['build_service/document/test/DocumentTestHelper.h'],
    include_prefix='build_service',
    strip_include_prefix='build_service',
    deps=[
        ':bs_document', ':bs_testbase',
        '//aios/storage/indexlib/index/common/field_format:attribute_field_format'
    ]
)
cc_test(
    name='bs_document_test',
    srcs=glob(['build_service/document/test/*.cpp']),
    copts=['-fno-access-control'],
    data=glob(['testdata/**']),
    deps=[':bs_document', ':bs_document_test_helper', ':bs_testbase']
)
cc_library(
    name='web_toolkit',
    srcs=glob([
        'build_service/web_toolkit/base/*.cpp',
        'build_service/web_toolkit/worker/*.cpp'
    ]),
    hdrs=glob([
        'build_service/web_toolkit/base/*.h',
        'build_service/web_toolkit/worker/*.h',
        'build_service/web_toolkit/third_party/inja/*.hpp',
        'build_service/web_toolkit/third_party/nlohmann/*.hpp'
    ]),
    include_prefix='build_service',
    strip_include_prefix='build_service',
    visibility=['//visibility:public'],
    deps=[
        '//aios/autil:NoCopyable', '//aios/autil:log', '//aios/autil:result',
        '//aios/filesystem/fslib:fslib-framework',
        '//aios/storage/indexlib/util:http_lib'
    ],
    alwayslink=True
)
cc_library(
    name='bs_admin',
    srcs=glob([
        'build_service/admin/*.cpp', 'build_service/admin/taskcontroller/*.cpp',
        'build_service/admin/controlflow/*.cpp',
        'build_service/admin/catalog/*.cpp'
    ],
              exclude=['build_service/admin/AdminWorker.cpp']),
    hdrs=glob([
        'build_service/admin/*.h', 'build_service/admin/taskcontroller/*.h',
        'build_service/admin/controlflow/*.h', 'build_service/admin/catalog/*.h'
    ]),
    include_prefix='build_service',
    strip_include_prefix='build_service',
    visibility=['//visibility:public'],
    deps=[
        ':bs_common', ':bs_config', ':bs_proto', ':bs_util',
        '//aios/autil:closure_guard', '//aios/autil:regex',
        '//aios/catalog/proto:catalog_entity_cc_proto',
        '//aios/catalog/proto:catalog_service_cc_proto',
        '//aios/filesystem/fslib:fslib-framework',
        '//aios/libcarbon3:master_framework', '//aios/network/leader_client',
        '//aios/storage:table_models',
        '//aios/storage/indexlib/file_system/wal',
        '//aios/storage/indexlib/framework/index_task:task_execute',
        '//aios/storage/indexlib/indexlib/partition:index_roll_back_util',
        '//aios/storage/indexlib/table/index_task:index_task_constant',
        '//third_party/lua', '@gperftools'
    ],
    alwayslink=True
)
cc_library(
    name='bs_worker_test_helper',
    hdrs=glob(['build_service/worker/test/Mock*.h']),
    include_prefix='build_service',
    strip_include_prefix='build_service',
    deps=[':bs_testbase', ':bs_worker']
)
cc_library(
    name='bs_config_test_helper',
    srcs=glob(['build_service/config/test/Fake*.cpp']),
    hdrs=glob(['build_service/config/test/Fake*.h']),
    include_prefix='build_service',
    strip_include_prefix='build_service',
    deps=[':bs_common_test_helper']
)
cc_library(
    name='bs_admin_test_helper',
    srcs=glob([
        'build_service/admin/test/Mock*.cpp',
        'build_service/admin/test/Fake*.cpp',
        'build_service/admin/test/GenerationTaskStateMachine.cpp',
        'build_service/admin/test/BuilderV2Node.cpp',
        'build_service/admin/test/SoReplaceGenerationTask.cpp'
    ]),
    hdrs=(
        glob([
            'build_service/admin/test/Mock*.h',
            'build_service/admin/test/Fake*.h'
        ]) + [
            'build_service/admin/test/FileUtilForTest.h',
            'build_service/admin/test/GenerationTaskStateMachine.h',
            'build_service/admin/test/BuilderV2Node.h',
            'build_service/admin/test/SoReplaceGenerationTask.h',
            'build_service/admin/test/FakeZkState.cpp'
        ]
    ),
    include_prefix='build_service',
    strip_include_prefix='build_service',
    deps=[
        ':bs_admin', ':bs_common_test_helper', ':bs_config_test_helper',
        ':bs_proto_test_helper', ':bs_testbase', ':bs_worker'
    ]
)
cc_library(
    name='bs_processor',
    srcs=glob(['build_service/processor/*.cpp']),
    hdrs=glob(['build_service/processor/*.h']),
    include_prefix='build_service',
    strip_include_prefix='build_service',
    visibility=['//visibility:public'],
    deps=[
        ':bs_analyzer', ':bs_basic', ':bs_document', ':bs_plugin',
        '//aios/autil:base64', '//aios/storage/indexlib/document:param',
        '//aios/storage/indexlib/document/normal:convertor',
        '//aios/storage/indexlib/document/normal/rewriter',
        '//aios/storage/indexlib/indexlib/index_base',
        '//aios/storage/indexlib/util:json_map'
    ],
    alwayslink=True
)
cc_library(
    name='bs_builder',
    srcs=glob(['build_service/builder/*.cpp']),
    hdrs=glob(['build_service/builder/*.h']),
    copts=[],
    include_prefix='build_service',
    strip_include_prefix='build_service',
    deps=([
        ':bs_common', '//aios/autil:mem_pool_container',
        '//aios/autil:mem_util', '//aios/storage/indexlib:interface',
        '//aios/storage/indexlib/config:sort_description',
        '//aios/storage/indexlib/indexlib/partition:indexlib_partition',
        '//aios/storage:table_models',
        '//aios/storage/indexlib/table/index_task:merge_task'
    ] + ['//aios/future_lite/future_lite/executors:simple_async_io_executor']),
    alwayslink=True
)
cc_library(
    name='bs_builder_test_helper',
    srcs=[
        'build_service/builder/test/FakeThread.cpp',
        'build_service/builder/test/IndexBuilderCreator.cpp'
    ],
    hdrs=glob(['build_service/builder/test/*.h']),
    copts=[],
    include_prefix='build_service',
    strip_include_prefix='build_service',
    deps=[':bs_builder', ':bs_testbase']
)
cc_test(
    name='bs_builder_test',
    srcs=glob(['build_service/builder/test/*Test.cpp'],
              exclude=['build_service/builder/test/BuilderRangeTest.cpp']),
    copts=['-fno-access-control'],
    data=glob(['testdata/**']),
    shard_count=5,
    deps=[
        ':bs_builder', ':bs_builder_test_helper', ':bs_document_test_helper',
        ':bs_testbase',
        '//aios/storage/indexlib/document/test:KVDocumentBatchMaker',
        '//aios/storage/indexlib/framework/mock:tablet',
        '//aios/storage/indexlib/indexlib:indexlib_testlib',
        '//aios/storage/indexlib/indexlib/test:document'
    ]
)
cc_library(
    name='bs_workflow',
    srcs=glob(['build_service/workflow/*.cpp']),
    hdrs=glob(['build_service/workflow/*.h']),
    include_prefix='build_service',
    strip_include_prefix='build_service',
    visibility=['//visibility:public'],
    deps=[
        ':bs_builder', ':bs_processor',
        '//aios/apps/facility/build_service/build_service/reader:bs_reader',
        '//aios/future_lite', '//aios/storage/indexlib:interface'
    ],
    alwayslink=True
)
cc_library(
    name='bs_custom_merger',
    srcs=glob(['build_service/custom_merger/*.cpp']),
    hdrs=glob(['build_service/custom_merger/*.h']),
    copts=[],
    include_prefix='build_service',
    strip_include_prefix='build_service',
    visibility=['//visibility:public'],
    deps=[
        ':bs_config', ':bs_plugin',
        '//aios/storage/indexlib/indexlib/partition:indexlib_partition'
    ],
    alwayslink=True
)
cc_library(
    name='bs_task_base',
    srcs=glob(['build_service/task_base/*.cpp'],
              exclude=['build_service/task_base/BuildInTaskFactory.cpp']),
    hdrs=glob(['build_service/task_base/*.h'],
              exclude=['build_service/task_base/BuildInTaskFactory.h']),
    copts=[],
    include_prefix='build_service',
    strip_include_prefix='build_service',
    visibility=['//visibility:public'],
    deps=[
        ':bs_custom_merger', ':bs_io', ':bs_workflow',
        '//aios/storage/indexlib/indexlib:indexlib_merger'
    ],
    alwayslink=True
)
cc_library(
    name='bs_build_in_task_factory',
    srcs=['build_service/task_base/BuildInTaskFactory.cpp'],
    hdrs=['build_service/task_base/BuildInTaskFactory.h'],
    include_prefix='build_service',
    strip_include_prefix='build_service',
    visibility=['//visibility:public'],
    deps=[':bs_build_task', ':bs_general_task', ':bs_task_base'],
    alwayslink=True
)
cc_library(
    name='bs_general_task',
    srcs=glob(['build_service/general_task/*.cpp']),
    hdrs=glob(['build_service/general_task/*.h']),
    copts=[],
    include_prefix='build_service',
    strip_include_prefix='build_service',
    visibility=['//visibility:public'],
    deps=([
        ':bs_basic', ':bs_config', ':bs_task_base', ':bs_util',
        '//aios/autil:env_util', '//aios/future_lite',
        '//aios/future_lite/future_lite/executors:simple_executor',
        '//aios/storage/indexlib/config:options',
        '//aios/storage/indexlib/config:schema',
        '//aios/storage/indexlib/framework:tablet',
        '//aios/storage/indexlib/framework/index_task:task_execute',
        '//aios/storage/indexlib/table/index_task:index_task_constant'
    ] + ['//aios/future_lite/future_lite/executors:simple_async_io_executor']),
    alwayslink=True
)
cc_library(
    name='bs_build_task',
    srcs=glob(['build_service/build_task/*.cpp']),
    hdrs=glob(['build_service/build_task/*.h']),
    copts=[],
    include_prefix='build_service',
    strip_include_prefix='build_service',
    visibility=['//visibility:public'],
    deps=[
        ':bs_basic', ':bs_common', ':bs_config', ':bs_general_task',
        ':bs_merge', ':bs_task_base', ':bs_util', '//aios/autil:env_util',
        '//aios/future_lite', '//aios/storage/indexlib/config:options',
        '//aios/storage/indexlib/config:schema',
        '//aios/storage/indexlib/framework:tablet',
        '//aios/storage/indexlib/framework:version_cleaner',
        '//aios/storage/indexlib/framework/index_task:task_execute',
        '//aios/storage/indexlib/table/index_task:index_task_constant',
        '//aios/storage/indexlib/table/index_task:merge_task',
        '//aios/storage/indexlib/util:ip_convertor'
    ],
    alwayslink=True
)
cc_library(
    name='bs_merge',
    srcs=glob(['build_service/merge/*.cpp']),
    hdrs=glob(['build_service/merge/*.h']),
    copts=[],
    include_prefix='build_service',
    strip_include_prefix='build_service',
    visibility=['//visibility:public'],
    deps=[
        ':bs_common', ':bs_proto', '//aios/autil:mem_pool_container',
        '//aios/autil:mem_util', '//aios/storage/indexlib:interface',
        '//aios/storage/indexlib/framework/index_task:task_execute',
        '//aios/storage/indexlib/table/index_task:index_task_constant',
        '//aios/storage/indexlib/table/index_task:merge_task'
    ],
    alwayslink=True
)
cc_library(
    name='mock_processor',
    hdrs=['build_service/processor/test/MockProcessor.h'],
    include_prefix='build_service',
    strip_include_prefix='build_service',
    visibility=['//visibility:public'],
    deps=[':bs_processor']
)
cc_library(
    name='mock_raw_document_rewriter',
    hdrs=['build_service/workflow/test/MockRawDocumentRewriter.h'],
    include_prefix='build_service',
    strip_include_prefix='build_service',
    visibility=['//visibility:public'],
    deps=[':bs_workflow']
)
cc_library(
    name='mock_builder_v2',
    hdrs=['build_service/builder/test/MockBuilderV2.h'],
    include_prefix='build_service',
    strip_include_prefix='build_service',
    visibility=['//visibility:public'],
    deps=[':bs_builder']
)
cc_library(
    name='bs_local_job_lib',
    srcs=glob(['build_service/local_job/*.cpp']),
    hdrs=glob(['build_service/local_job/*.h']),
    include_prefix='build_service',
    strip_include_prefix='build_service',
    visibility=['//visibility:public'],
    deps=[
        ':bs_task_base', '//aios/autil:env_util', '//aios/future_lite',
        '//aios/storage/indexlib/config:options',
        '//aios/storage/indexlib/config:schema',
        '//aios/storage/indexlib/framework:tablet',
        '//aios/storage/indexlib/framework/index_task:task_execute',
        '//aios/storage/indexlib/indexlib:third_party_lib',
        '//aios/storage/indexlib/table/index_task:index_task_constant',
        '//aios/storage/indexlib/table/index_task:merge_task'
    ]
)
cc_binary(
    name='bs_local_job',
    visibility=['//visibility:public'],
    deps=[':bs_local_job_lib', '//aios/network/gig:multi_call']
)
cc_library(
    name='bs_worker',
    srcs=glob(['build_service/worker/*.cpp']),
    hdrs=glob(['build_service/worker/*.h']),
    include_prefix='build_service',
    strip_include_prefix='build_service',
    visibility=['//visibility:public'],
    deps=[
        ':bs_build_in_task_factory', ':bs_common', ':bs_proto', ':bs_task_base',
        '//aios/alog', '//aios/autil:closure_guard', '//aios/autil:crc32c',
        '//aios/hippo:hippo_sdk_header', '//aios/network/arpc',
        '//aios/storage/indexlib/indexlib:third_party_lib',
        '//aios/storage/indexlib/indexlib/partition:index_roll_back_util'
    ],
    alwayslink=True
)
cc_binary(
    name='bs_admin_worker',
    srcs=glob(['build_service/admin/AdminWorker.cpp']),
    tags=['fuse-keep-symbol', 'manual', 'no-remote-exec'],
    visibility=['//visibility:public'],
    deps=[
        ':bs_admin', '//aios/worker_framework:worker_base_main', '@gperftools'
    ]
)
cc_binary(
    name='build_service_worker',
    srcs=['@com_google_protobuf//:protobuf'],
    tags=['fuse-keep-symbol', 'manual', 'no-remote-exec'],
    visibility=['//visibility:public'],
    deps=[
        ':bs_worker', '//aios/network/gig:multi_call',
        '//aios/worker_framework:worker_base_main'
    ]
)
bundle_files(
    name='bs_etc_package',
    srcs=[':install_etcs'],
    prefix='usr/local/etc/bs',
    strip_prefix='misc',
    tags=['manual'],
    visibility=['//visibility:public']
)
bundle_files(
    name='bs_lua_conf_package',
    srcs=[':bs_lua_conf_file'],
    prefix='usr/local/etc/bs/lua',
    tags=['manual'],
    visibility=['//visibility:public']
)
bundle_files(
    name='bs_lua_script_package',
    srcs=[
        '//aios/apps/facility/build_service/build_service/admin/controlflow/lua:bs_lua_script'
    ],
    prefix='usr/local/etc/bs/lua',
    tags=['manual'],
    visibility=['//visibility:public']
)
bundle_files(
    name='elfutils_so',
    srcs=['//third_party/elfutils-libelf:elfutils-libelf_shared'],
    prefix='usr/local/lib64',
    visibility=['//visibility:public']
)
bundle_files(
    name='bs_tools_etc_package',
    srcs=[],
    prefix='usr/local/etc',
    tags=['manual'],
    visibility=['//visibility:public'],
    deps=[
        ':elfutils_so', '//aios/apps/facility/build_service/tools:install_etcs'
    ]
)
bundle_files(
    name='bs_tools_include_python_package',
    srcs=['//aios/apps/facility/build_service/tools:bs_tools_include'],
    prefix='usr/local/lib/python/site-packages',
    tags=['manual']
)
bundle_files(
    name='bs_tools_python_package',
    srcs=['//aios/apps/facility/build_service/tools:bs_tools'],
    prefix='usr/local/lib/python/site-packages',
    tags=['manual'],
    visibility=['//visibility:public'],
    deps=[':bs_tools_include_python_package']
)
