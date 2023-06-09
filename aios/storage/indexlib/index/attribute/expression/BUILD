load('//aios/storage/indexlib:defs.bzl', 'indexlib_cc_library')
package(
    default_visibility=[
        '//aios/storage/indexlib/index/attribute/expression:__subpackages__'
    ]
)
indexlib_cc_library(
    name='evaluator',
    srcs=[],
    hdrs=[],
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[':DocumentEvaluator', ':DocumentEvaluatorMaintainer']
)
indexlib_cc_library(
    name='config',
    srcs=[],
    hdrs=[],
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[':FunctionConfig']
)
indexlib_cc_library(name='FunctionConfig', deps=['//aios/autil:json'])
indexlib_cc_library(
    name='DocumentEvaluator',
    srcs=[],
    deps=[
        ':AtomicAttributeExpression', '//aios/autil:NoCopyable',
        '//aios/storage/indexlib/base:Status',
        '//aios/storage/indexlib/base:Types'
    ]
)
indexlib_cc_library(
    name='DocumentEvaluatorMaintainer',
    deps=[
        ':AtomicExpressionCreator', ':BuiltinFunctionFactory',
        ':DocumentEvaluator', ':FunctionConfig', '//aios/autil:NoCopyable',
        '//aios/expression', '//aios/storage/indexlib/base:Status',
        '//aios/storage/indexlib/base:Types',
        '//aios/storage/indexlib/config:schema',
        '//aios/storage/indexlib/index/attribute:constants'
    ]
)
indexlib_cc_library(
    name='TabletSessionResource',
    srcs=[],
    deps=['//aios/expression', '//aios/storage/indexlib/framework:Segment']
)
indexlib_cc_library(
    name='AtomicExpressionCreator',
    hdrs=['AtomicExpressionCreator.h', 'TabletSessionResource.h'],
    deps=[
        ':AtomicAttributeExpression', '//aios/expression',
        '//aios/storage/indexlib/base:FieldTypeUtil',
        '//aios/storage/indexlib/config:TabletSchema',
        '//aios/storage/indexlib/index/attribute:config',
        '//aios/storage/indexlib/index/attribute:constants'
    ]
)
indexlib_cc_library(
    name='AtomicAttributeExpression',
    srcs=[],
    deps=[
        ':AttributePrefetcher', ':TabletSessionResource', '//aios/expression'
    ]
)
indexlib_cc_library(
    name='AttributePrefetcher',
    srcs=[],
    deps=[
        '//aios/autil:memory', '//aios/expression',
        '//aios/storage/indexlib/framework:Segment',
        '//aios/storage/indexlib/index/attribute:AttributeIndexFactory',
        '//aios/storage/indexlib/index/attribute:reader'
    ]
)
indexlib_cc_library(
    name='BuiltinFunctionFactory',
    deps=[
        ':BuiltinFunctionInterface', '//aios/autil:time', '//aios/expression'
    ]
)
indexlib_cc_library(
    name='BuiltinFunctionInterface', srcs=[], deps=['//aios/expression']
)
