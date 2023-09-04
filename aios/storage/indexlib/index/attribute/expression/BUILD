load('//aios/storage:defs.bzl', 'strict_cc_library')
package(
    default_visibility=[
        '//aios/storage/indexlib/index/attribute/expression:__subpackages__'
    ]
)
strict_cc_library(
    name='evaluator',
    srcs=[],
    hdrs=[],
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[':DocumentEvaluator', ':DocumentEvaluatorMaintainer']
)
strict_cc_library(
    name='config',
    srcs=[],
    hdrs=[],
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[':FunctionConfig']
)
strict_cc_library(name='FunctionConfig', deps=['//aios/autil:json'])
strict_cc_library(
    name='DocumentEvaluator',
    srcs=[],
    deps=[
        ':AtomicAttributeExpression', '//aios/autil:NoCopyable',
        '//aios/storage/indexlib/base:Status',
        '//aios/storage/indexlib/base:Types'
    ]
)
strict_cc_library(
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
strict_cc_library(
    name='TabletSessionResource',
    srcs=[],
    deps=['//aios/expression', '//aios/storage/indexlib/framework:Segment']
)
strict_cc_library(
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
strict_cc_library(
    name='AtomicAttributeExpression',
    srcs=[],
    deps=[
        ':AttributePrefetcher', ':TabletSessionResource', '//aios/expression'
    ]
)
strict_cc_library(
    name='AttributePrefetcher',
    srcs=[],
    deps=[
        '//aios/autil:memory', '//aios/expression',
        '//aios/storage/indexlib/framework:Segment',
        '//aios/storage/indexlib/index/attribute:AttributeIndexFactory',
        '//aios/storage/indexlib/index/attribute:reader'
    ]
)
strict_cc_library(
    name='BuiltinFunctionFactory',
    deps=[
        ':BuiltinFunctionInterface', '//aios/autil:time', '//aios/expression'
    ]
)
strict_cc_library(
    name='BuiltinFunctionInterface', srcs=[], deps=['//aios/expression']
)
