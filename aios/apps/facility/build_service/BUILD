load('@rules_cc//cc:defs.bzl', 'cc_binary', 'cc_library', 'cc_test')
load(
    '//bazel:defs.bzl', 'cc_proto', 'gen_conf', 'gen_conf_lib', 'genlex',
    'test_cc_plugin_shared_library'
)
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
genlex(name='lexer', src='build_service/analyzer/SingleWSScanner.ll')
cc_proto(
    name='proto',
    srcs=glob(['build_service/proto/*.proto']),
    import_prefix='build_service/proto',
    strip_import_prefix='build_service/proto',
    visibility=['//visibility:public'],
    deps=['//aios/hippo:proto', '//aios/network/arpc:proto']
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
    name='bs_proto',
    srcs=glob(['build_service/proto/*.cpp']),
    hdrs=glob(['build_service/proto/*.h']),
    include_prefix='build_service',
    strip_include_prefix='build_service',
    deps=[
        ':bs_basic', ':proto_cc_proto', '//aios/autil:range_util',
        '//aios/network/http_arpc'
    ],
    alwayslink=True
)
cc_proto(
    name='proto_test',
    srcs=glob(['build_service/proto/test/*.proto']),
    import_prefix='build_service/proto',
    strip_import_prefix='build_service/proto',
    visibility=['//visibility:public'],
    deps=['//aios/hippo:proto', '//aios/network/arpc:proto']
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
        '@swift//:swift', '//aios/autil:string_helper',
        '//aios/storage/indexlib/framework:id_generator',
        '//third_party/hiredis'
    ],
    alwayslink=True
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
cc_library(
    name='bs_plugin_config',
    hdrs=['build_service/plugin/ModuleInfo.h'],
    include_prefix='build_service',
    strip_include_prefix='build_service',
    visibility=['//visibility:public'],
    deps=['//aios/storage/indexlib/indexlib:indexlib_plugin_config']
)
cc_library(
    name='bs_config',
    srcs=glob(['build_service/config/*.cpp'],
              exclude=[
                  'build_service/config/HashMode.cpp',
                  'build_service/config/CLIOptionNames.cpp'
              ]),
    hdrs=glob(['build_service/config/*.h']),
    include_prefix='build_service',
    strip_include_prefix='build_service',
    visibility=['//visibility:public'],
    deps=[
        ':bs_hashmode', ':bs_plugin_config', ':bs_proto', ':bs_util',
        '//aios/filesystem/fslib:fslib-framework',
        '//aios/storage/indexlib/config:options',
        '//aios/storage/indexlib/config:schema',
        '//aios/storage/indexlib/framework:tablet_factory',
        '//aios/storage/indexlib/framework:tablet_schema_loader',
        '//aios/storage/indexlib/indexlib/config'
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
        ':bs_config', ':bs_proto',
        '//aios/storage/indexlib/framework/index_task:task_execute',
        '//aios/storage/indexlib/indexlib',
        '//aios/storage/indexlib/table/normal_table/index_task:document_reclaim',
        '//aios/worker_framework'
    ],
    alwayslink=True
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
        '//aios/storage/indexlib/indexlib/index_base'
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
        '//aios/storage/indexlib/table:all',
        '//aios/storage/indexlib/table/index_task:merge_task'
    ] + ['//aios/future_lite/future_lite/executors:simple_async_io_executor']),
    alwayslink=True
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
bundle_files(
    name='bs_tools_etc_package',
    srcs=[],
    prefix='usr/local/etc',
    tags=['manual'],
    visibility=['//visibility:public'],
    deps=['//aios/apps/facility/build_service/tools:install_etcs']
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
