package(default_visibility=['//aios/suez_turing:__subpackages__'])
load('//bazel:defs.bzl', 'test_bc_code_multi')
load('//bazel:defs.bzl', 'gen_cpp_code')
cc_library(
    name='turing_expression_headers',
    hdrs=glob([
        'util/*.h', 'syntax/*.h', 'framework/*.h', 'function/*.h',
        'provider/*.h', 'plugin/*.h', 'cava/common/*.h', 'cava/impl/*.h',
        'cava/ast/*.h'
    ],
              exclude=[
                  'cava/common/CavaJitWrapperR.h',
                  'cava/common/CavaPluginManagerR.h',
                  'function/FunctionInterfaceCreatorR.h',
                  'plugin/SorterManagerR.h', 'plugin/RankManagerR.h',
                  'framework/ExprConfigManagerR.h'
              ]),
    deps=[
        ':common_h',
        '//aios/suez_turing/suez/turing/expression/util/index_info:index_info',
        '//aios/suez_turing/suez/turing/expression/provider/matchdata:matchdata',
        '//aios/suez_turing:closure', '//aios/cava:cava_adapter',
        '//aios/matchdoc:matchdoc', '//aios/turing_ops_util:sort_expr_meta',
        '//aios/turing_ops_util:op_doc_tracer'
    ],
    visibility=[':__subpackages__', '//aios/igraph:__subpackages__'],
    include_prefix='suez/turing/expression'
)
cc_library(
    name='suez_resource_reader',
    hdrs=['config/ResourceReader.h'],
    deps=['//aios/apps/facility/build_service:bs_plugin'],
    include_prefix='suez'
)
cc_library(
    name='turing_expression_util',
    srcs=glob(['util/*.cpp']),
    deps=[
        ':turing_expression_headers',
        '//aios/suez_turing/suez/turing/expression/util/index_info:index_info',
        '//aios/storage/indexlib/indexlib/partition:reader_sdk',
        '//aios/turing_ops_util:trace'
    ],
    visibility=['//visibility:public'],
    alwayslink=1
)
cc_library(
    name='turing_expression_syntax_without_validator',
    hdrs=glob(['syntax/*.h']),
    srcs=(
        glob(['syntax/*.cpp'], exclude=['syntax/SyntaxExprValidator.cpp']) + [
            '//aios/suez_turing/suez/turing/expression/syntax/parser:scanner',
            '//aios/suez_turing/suez/turing/expression/syntax/parser:parser_srcs'
        ]
    ),
    deps=[
        ':common_h',
        '//aios/suez_turing/suez/turing/expression/syntax/parser:parser_hdrs'
    ],
    include_prefix='suez/turing/expression',
    visibility=['//visibility:public']
)
cc_library(
    name='turing_expression_syntax',
    srcs=['syntax/SyntaxExprValidator.cpp'],
    deps=[
        ':turing_expression_headers', ':turing_expression_util',
        ':turing_expression_syntax_without_validator'
    ],
    include_prefix='suez/turing/expression',
    visibility=['//visibility:public'],
    alwayslink=1
)
template_header = '\n#include "suez/turing/expression/framework/AttributeExpressionFactory.impl.hpp"\n\nnamespace suez::turing {\n'
template_body = '\nIMPL_ONE_ATOMIC_ATTR_EXPR_TYPED({0}, {1}, {2});\n'
template_tail = '\n}\n'
gen_cpp_code(
    'attribute_expression_factory_generated',
    [[
        'vt_int8', 'vt_int16', 'vt_int32', 'vt_int64', 'vt_uint8', 'vt_uint16',
        'vt_uint32', 'vt_uint64', 'vt_float', 'vt_double', 'vt_string',
        'vt_hash_128'
    ], ['DefaultDocIdAccessor', 'JoinDocIdAccessor', 'SubDocIdAccessor'],
     [
         'indexlib::index::AttributeReaderPtr',
         'std::shared_ptr<indexlibv2::index::AttributeReader>'
     ]],
    template_header,
    template_body,
    template_tail,
    element_per_file=1
)
cc_library(
    name='attribute_expression_factory',
    srcs=[':attribute_expression_factory_generated'],
    hdrs=glob(['framework/AttributeExpressionFactory.impl.hpp']),
    deps=[
        ':turing_expression_headers', ':turing_expression_syntax',
        ':suez_resource_reader', '//aios/cava:cava_adapter',
        '//aios/turing_ops_util:trace', '//aios/network/gig:multi_call',
        '//aios/resource_reader:resource_reader',
        '//aios/apps/facility/build_service:bs_plugin',
        '//aios/storage/indexlib/indexlib/partition:reader_sdk',
        '//aios/matchdoc:matchdoc', '//aios/kmonitor:kmonitor_client_cpp',
        '//aios/suez_turing/suez/turing/expression/provider/matchdata:matchdata',
        '//aios/storage/indexlib/index/attribute:reader',
        '@boost//:preprocessor-headers'
    ],
    include_prefix='suez/turing/expression'
)
cc_library(
    name='turing_expression',
    srcs=glob([
        'framework/*.cpp', 'function/*.cpp', 'provider/*.cpp', 'plugin/*.cpp',
        'cava/common/*.cpp', 'cava/impl/*.cpp', 'cava/ast/*.cpp'
    ],
              exclude=[
                  'AttributeExpressionFactory.in.cpp',
                  'AttributeExpressionFactory.impl.hpp',
                  'cava/common/CavaJitWrapperR.cpp',
                  'cava/common/CavaPluginManagerR.cpp',
                  'function/FunctionInterfaceCreatorR.cpp',
                  'plugin/SorterManagerR.cpp', 'plugin/RankManagerR.cpp',
                  'framework/ExprConfigManagerR.cpp'
              ]),
    deps=[
        ':attribute_expression_factory', ':turing_expression_headers',
        ':turing_expression_syntax', ':suez_resource_reader',
        '//aios/cava:cava_adapter', '//aios/turing_ops_util:trace',
        '//aios/network/gig:multi_call',
        '//aios/resource_reader:resource_reader',
        '//aios/apps/facility/build_service:bs_plugin',
        '//aios/storage/indexlib/indexlib/partition:reader_sdk',
        '//aios/matchdoc:matchdoc', '//aios/kmonitor:kmonitor_client_cpp',
        '//aios/suez_turing/suez/turing/expression/provider/matchdata:matchdata',
        '//aios/storage/indexlib/index/attribute:reader',
        '@boost//:preprocessor-headers'
    ],
    visibility=['//visibility:public'],
    alwayslink=1
)
cc_library(
    name='turing_expression_navi_resource',
    srcs=[
        'cava/common/CavaJitWrapperR.cpp', 'cava/common/CavaPluginManagerR.cpp',
        'function/FunctionInterfaceCreatorR.cpp', 'plugin/SorterManagerR.cpp',
        'plugin/RankManagerR.cpp', 'framework/ExprConfigManagerR.cpp'
    ],
    hdrs=[
        'cava/common/CavaJitWrapperR.h', 'cava/common/CavaPluginManagerR.h',
        'function/FunctionInterfaceCreatorR.h',
        'function/FunctionFactoryBaseR.h', 'plugin/SorterManagerR.h',
        'plugin/RankManagerR.h', 'framework/ExprConfigManagerR.h'
    ],
    deps=[
        '//aios/navi', '//aios/kmonitor:metrics_report_navi_resource',
        '//aios/suez_turing/suez/turing/expression:turing_expression'
    ],
    include_prefix='suez/turing/expression',
    visibility=['//visibility:public'],
    alwayslink=1
)
cc_library(
    name='common_h',
    hdrs=['common.h'],
    deps=[
        '//aios/autil:hash_base', '//aios/autil:multi_value',
        '//aios/matchdoc:matchdoc'
    ],
    visibility=['//visibility:public'],
    include_prefix='suez/turing/expression'
)
test_bc_code_multi(
    name='ha3_cava',
    srcs=[
        'cava/impl/Ha3CavaScorerParam.cpp', 'cava/impl/CavaMultiValueTyped.cpp',
        'cava/impl/FieldRef.cpp', 'cava/impl/TraceApi.cpp',
        'cava/impl/AttributeExpression.cpp', 'cava/impl/Reference.cpp',
        'cava/impl/MatchDocs.cpp'
    ],
    deps=[':ha3_cava_bc_hdrs', '//aios/turing_ops_util:trace'],
    includes=['aios/storage'],
    copy_name='ha3_cava.bc',
    dests=['testdata/bitcode/'],
    visibility=['//visibility:public']
)
