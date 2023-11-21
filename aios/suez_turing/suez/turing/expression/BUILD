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
                  'function/FunctionFactoryBaseR.h',
                  'function/FunctionInterfaceCreatorR.h',
                  'plugin/SorterManagerR.h', 'plugin/RankManagerR.h',
                  'framework/ExprConfigManagerR.h'
              ]),
    include_prefix='suez/turing/expression',
    visibility=[':__subpackages__', '//aios/igraph:__subpackages__'],
    deps=[
        ':common_h', '//aios/cava:cava_adapter', '//aios/matchdoc',
        '//aios/suez_turing:closure',
        '//aios/suez_turing/suez/turing/expression/provider/matchdata',
        '//aios/suez_turing/suez/turing/expression/util/index_info',
        '//aios/turing_ops_util:op_doc_tracer',
        '//aios/turing_ops_util:sort_expr_meta'
    ]
)
cc_library(
    name='suez_resource_reader',
    hdrs=['config/ResourceReader.h'],
    include_prefix='suez',
    deps=['//aios/apps/facility/build_service:bs_plugin']
)
cc_library(
    name='turing_expression_util',
    srcs=glob(['util/*.cpp']),
    visibility=['//visibility:public'],
    deps=[
        ':turing_expression_headers',
        '//aios/storage/indexlib/indexlib/partition:reader_sdk',
        '//aios/suez_turing/suez/turing/expression/util/index_info',
        '//aios/turing_ops_util:trace'
    ],
    alwayslink=1
)
cc_library(
    name='turing_expression_syntax_without_validator',
    srcs=(
        glob(['syntax/*.cpp'], exclude=['syntax/SyntaxExprValidator.cpp']) + [
            '//aios/suez_turing/suez/turing/expression/syntax/parser:scanner',
            '//aios/suez_turing/suez/turing/expression/syntax/parser:parser_srcs'
        ]
    ),
    hdrs=glob(['syntax/*.h']),
    include_prefix='suez/turing/expression',
    visibility=['//visibility:public'],
    deps=[
        ':common_h',
        '//aios/suez_turing/suez/turing/expression/syntax/parser:parser_hdrs'
    ]
)
cc_library(
    name='turing_expression_syntax',
    srcs=['syntax/SyntaxExprValidator.cpp'],
    include_prefix='suez/turing/expression',
    visibility=['//visibility:public'],
    deps=[
        ':turing_expression_headers', ':turing_expression_util',
        ':turing_expression_syntax_without_validator'
    ],
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
    include_prefix='suez/turing/expression',
    deps=[
        ':suez_resource_reader', ':turing_expression_headers',
        ':turing_expression_syntax',
        '//aios/apps/facility/build_service:bs_plugin',
        '//aios/cava:cava_adapter', '//aios/kmonitor:kmonitor_client_cpp',
        '//aios/matchdoc', '//aios/network/gig:multi_call',
        '//aios/resource_reader',
        '//aios/storage/indexlib/index/attribute:reader',
        '//aios/storage/indexlib/indexlib/partition:reader_sdk',
        '//aios/suez_turing/suez/turing/expression/provider/matchdata',
        '//aios/turing_ops_util:trace', '@boost//:preprocessor-headers'
    ]
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
                  'function/FunctionFactoryBaseR.cpp',
                  'function/FunctionInterfaceCreatorR.cpp',
                  'plugin/SorterManagerR.cpp', 'plugin/RankManagerR.cpp',
                  'framework/ExprConfigManagerR.cpp'
              ]),
    visibility=['//visibility:public'],
    deps=[
        ':attribute_expression_factory', ':turing_expression_headers',
        ':turing_expression_syntax', ':suez_resource_reader',
        '//aios/cava:cava_adapter', '//aios/turing_ops_util:trace',
        '//aios/network/gig:multi_call', '//aios/resource_reader',
        '//aios/apps/facility/build_service:bs_plugin',
        '//aios/storage/indexlib/indexlib/partition:reader_sdk',
        '//aios/matchdoc', '//aios/kmonitor:kmonitor_client_cpp',
        '//aios/suez_turing/suez/turing/expression/provider/matchdata',
        '//aios/storage/indexlib/index/attribute:reader',
        '@boost//:preprocessor-headers'
    ],
    alwayslink=1
)
cc_library(
    name='turing_expression_navi_resource',
    srcs=[
        'cava/common/CavaJitWrapperR.cpp', 'cava/common/CavaPluginManagerR.cpp',
        'framework/ExprConfigManagerR.cpp', 'function/FunctionFactoryBaseR.cpp',
        'function/FunctionInterfaceCreatorR.cpp', 'plugin/RankManagerR.cpp',
        'plugin/SorterManagerR.cpp'
    ],
    hdrs=[
        'cava/common/CavaJitWrapperR.h', 'cava/common/CavaPluginManagerR.h',
        'framework/ExprConfigManagerR.h', 'function/FunctionFactoryBaseR.h',
        'function/FunctionInterfaceCreatorR.h', 'plugin/RankManagerR.h',
        'plugin/SorterManagerR.h'
    ],
    include_prefix='suez/turing/expression',
    visibility=['//visibility:public'],
    deps=[
        '//aios/kmonitor:metrics_report_navi_resource', '//aios/navi',
        '//aios/suez_turing/suez/turing/expression:turing_expression'
    ],
    alwayslink=1
)
cc_library(
    name='common_h',
    hdrs=['common.h'],
    include_prefix='suez/turing/expression',
    visibility=['//visibility:public'],
    deps=[
        '//aios/autil:hash_base', '//aios/autil:multi_value', '//aios/matchdoc'
    ]
)
test_bc_code_multi(
    name='ha3_cava',
    srcs=[
        'cava/impl/AttributeExpression.cpp',
        'cava/impl/CavaMultiValueTyped.cpp', 'cava/impl/FieldRef.cpp',
        'cava/impl/Ha3CavaScorerParam.cpp', 'cava/impl/MatchDocs.cpp',
        'cava/impl/Reference.cpp', 'cava/impl/TraceApi.cpp'
    ],
    copy_name='ha3_cava.bc',
    dests=['testdata/bitcode/'],
    includes=['aios/storage'],
    visibility=['//visibility:public'],
    deps=[':ha3_cava_bc_hdrs', '//aios/turing_ops_util:trace']
)
