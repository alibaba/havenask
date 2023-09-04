package(default_visibility=['//aios/sql:__subpackages__'])
cc_library(
    name='sql_ops_normal_scan',
    include_prefix='sql/ops/scan',
    visibility=['//visibility:public'],
    deps=[':normal_scan_r', ':open_source_ops'],
    alwayslink=True
)
cc_library(name='open_source_ops', deps=[])
cc_library(
    name='sql_ops_scan_base',
    srcs=glob(['*.cpp'],
              exclude=[
                  'NormalScan.cpp', 'NormalScanR.cpp', 'ScanR.cpp',
                  'ScanInitParamR.cpp', 'UseSubR.cpp',
                  'AttributeExpressionCreatorR.cpp', 'Collector.cpp',
                  'SummaryScanR.cpp', 'KKVScanR.cpp', 'KVScanR.cpp',
                  'AggregationScanR.cpp', 'AggIndexKeyCollector.cpp'
              ]),
    hdrs=glob(['*.h', '*.hpp'],
              exclude=[
                  'NormalScan.h', 'NormalScanR.h', 'ScanR.h',
                  'ScanInitParamR.h', 'UseSubR.h',
                  'AttributeExpressionCreatorR.h', 'Collector.h',
                  'SummaryScanR.h', 'KKVScanR.h', 'KVScanR.h',
                  'AggregationScanR.h', 'AggIndexKeyCollector.h'
              ]),
    include_prefix='sql/ops/scan',
    deps=[
        ':sql_ops_scan_r', '//aios/autil:object_tracer',
        '//aios/autil:plugin_base', '//aios/autil:range_util',
        '//aios/ha3:ha3_proto_basic_def_cc_proto_headers',
        '//aios/ha3/ha3/queryparser:ha3_queryparser',
        '//aios/ha3/ha3/search/auxiliary_chain:ha3_auxiliary_chain',
        '//aios/ha3/ha3/search/single_layer_searcher:ha3_single_layer_searcher',
        '//aios/ha3/ha3/turing/common/metadata:modelconfig', '//aios/navi_ops',
        '//aios/sql/common:sql_common',
        '//aios/sql/ops/calc:sql_ops_calc_table',
        '//aios/sql/ops/scan/udf_to_query:sql_ops_udf_to_query',
        '//aios/sql/ops/sort:sql_ops_sort_init_param',
        '//aios/sql/ops/tvf:sql_ops_tvf_wrapper',
        '//aios/sql/ops/util:sql_ops_util', '//aios/sql/resource:sql_resource',
        '//aios/storage/indexlib/indexlib/index:kkv',
        '//aios/storage/indexlib/table/kv_table:table',
        '//aios/suez:suez_search_sdk'
    ],
    alwayslink=True
)
cc_library(
    name='normal_scan_r',
    srcs=glob([
        'NormalScanR.cpp', 'SummaryScanR.cpp', 'KVScanR.cpp', 'KKVScanR.cpp'
    ]),
    hdrs=glob(['NormalScanR.h', 'SummaryScanR.h', 'KVScanR.h', 'KKVScanR.h']),
    include_prefix='sql/ops/scan',
    deps=[
        ':sql_ops_scan_base', '//aios/ha3/ha3/queryparser:ha3_queryparser',
        '//aios/sql/common:sql_common',
        '//aios/sql/ops/scan/scan_visitor:sql_ops_scan_visitor',
        '//aios/sql/resource:sql_resource',
        '//aios/suez_turing/suez/turing/navi:core'
    ],
    alwayslink=True
)
cc_library(
    name='sql_ops_scan_r',
    srcs=glob([
        'ScanR.cpp', 'ScanInitParamR.cpp', 'UseSubR.cpp',
        'AttributeExpressionCreatorR.cpp', 'Collector.cpp'
    ]),
    hdrs=glob([
        'ScanR.h', 'ScanInitParamR.h', 'UseSubR.h',
        'AttributeExpressionCreatorR.h', 'Collector.h'
    ]),
    include_prefix='sql/ops/scan',
    deps=[
        '//aios/sql/ops/scan/udf_to_query:sql_ops_udf_to_query',
        '//aios/sql/ops/sort:sql_ops_sort_init_param',
        '//aios/sql/resource:sql_resource',
        '//aios/suez_navi:suez_navi_resource',
        '//aios/suez_turing/suez/turing/navi:core'
    ],
    alwayslink=True
)
cc_library(
    name='fake_tokenizer',
    testonly=True,
    hdrs=['test/FakeTokenizer.h'],
    include_prefix='sql/ops/scan',
    deps=[
        ':sql_ops_scan_base',
        '//aios/sql/ops/scan/scan_visitor:sql_ops_scan_visitor',
        '//aios/sql/ops/test:ops_testlib',
        '//aios/storage/indexlib/indexlib:indexlib_testlib',
        '//aios/unittest_framework'
    ]
)
cc_library(
    name='condition_test_util',
    testonly=True,
    srcs=['test/ScanConditionTestUtil.cpp'],
    hdrs=['test/ScanConditionTestUtil.h'],
    include_prefix='sql/ops/scan',
    deps=[
        ':fake_tokenizer', ':sql_ops_scan_base',
        '//aios/sql/ops/scan/scan_visitor:sql_ops_scan_visitor',
        '//aios/storage/indexlib/indexlib:indexlib_testlib'
    ]
)
cc_test(
    name='scan_resource_test',
    srcs=glob(['test/*.cpp'],
              exclude=[
                  'test/AggregationScanRTest.cpp',
                  'test/ScanConditionTestUtil.cpp'
              ]),
    copts=['-fno-access-control'],
    data=['//aios/sql:testdata'],
    linkopts=['-Wl,--as-needed'],
    shard_count=3,
    deps=[
        ':normal_scan_r', '//aios/navi:navi_engine_testlib',
        '//aios/sql/ops/test:ops_testlib',
        '//aios/sql/resource/testlib:mock_tablet_manager_r',
        '//aios/storage/indexlib/table/kv_table/test:kv_table_test_helper',
        '//aios/unittest_framework'
    ]
)
