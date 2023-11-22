load('//bazel:defs.bzl', 'gen_cpp_code')
cc_library(
    name='trace',
    srcs=['turing_ops_util/variant/Tracer.cpp'],
    hdrs=['turing_ops_util/variant/Tracer.h'],
    include_prefix='turing_ops_util',
    strip_include_prefix='turing_ops_util',
    visibility=['//visibility:public'],
    deps=([
        '//aios/autil:common_macros', '//aios/autil:data_buffer',
        '//aios/autil:env_util', '//aios/matchdoc:matchdoc', '//aios/autil:log'
    ] + [])
)
cc_library(
    name='sort_expr_meta',
    hdrs=['turing_ops_util/variant/SortExprMeta.h'],
    include_prefix='turing_ops_util',
    strip_include_prefix='turing_ops_util',
    visibility=['//visibility:public'],
    deps=[],
    alwayslink=1
)
cc_library(
    name='op_doc_tracer',
    srcs=glob(['turing_ops_util/util/DocTracer.cpp']),
    hdrs=glob(['turing_ops_util/util/DocTracer.h']),
    include_prefix='turing_ops_util',
    strip_include_prefix='turing_ops_util',
    visibility=['//visibility:public'],
    deps=[':trace', '//aios/matchdoc'],
    alwayslink=1
)
template_header_fill_attr = '\n#include "turing_ops_util/util/AttributeToTensorFillAttr.impl.hpp"\n\nnamespace suez::turing {\n'
template_header_require_attr = '\n#include "turing_ops_util/util/AttributeToTensorRequireAttr.impl.hpp"\n\nnamespace suez::turing {\n'
template_body_fill_attr = '\nIMPL_ONE_FILE_FILL_ATTRIBUTE_TYPED({0}, {1}, {2});\n'
template_body_fill_attr2 = '\nIMPL_ONE_FILE_FILL_ATTRIBUTE_TYPED2({0}, {1}, {2});\n'
template_body_require_attr = '\nIMPL_ONE_FILE_REQUIRE_ATTRIBUTE_TYPED({0}, {1}, {2});\n'
template_tail = '\n}\n'
gen_cpp_code(
    'attribute_to_tensor_fill_attr_generated',
    [[
        'AT_INT8', 'AT_INT16', 'AT_INT32', 'AT_INT64', 'AT_UINT8', 'AT_UINT16',
        'AT_UINT32', 'AT_UINT64', 'AT_FLOAT', 'AT_DOUBLE'
    ], ['DocIdFromVec', 'DocIdFromMatchDocs', 'DocIdFromMatchDocsV2'],
     [
         'indexlib::index::AttributeReader',
         'indexlibv2::index::AttributeReader',
         'indexlib::index::PackAttributeReader'
     ]],
    template_header_fill_attr,
    template_body_fill_attr,
    template_tail,
    element_per_file=1
)
gen_cpp_code(
    'attribute_to_tensor_fill_attr_generated2',
    [['char', 'autil::MultiChar'],
     ['DocIdFromVec', 'DocIdFromMatchDocs', 'DocIdFromMatchDocsV2'],
     [
         'indexlib::index::AttributeReader',
         'indexlibv2::index::AttributeReader',
         'indexlib::index::PackAttributeReader'
     ]],
    template_header_fill_attr,
    template_body_fill_attr2,
    template_tail,
    element_per_file=1
)
gen_cpp_code(
    'attribute_to_tensor_fill_attr_generated_remote',
    [[
        'AT_INT8', 'AT_INT16', 'AT_INT32', 'AT_INT64', 'AT_UINT8', 'AT_UINT16',
        'AT_UINT32', 'AT_UINT64', 'AT_FLOAT', 'AT_DOUBLE'
    ], ['DocIdFromVec', 'DocIdFromMatchDocs', 'DocIdFromMatchDocsV2'],
     ['remote_indexlib::reader::RemoteAttributeReader']],
    template_header_fill_attr,
    template_body_fill_attr,
    template_tail,
    element_per_file=1
)
gen_cpp_code(
    'attribute_to_tensor_fill_attr_generated2_remote',
    [['char', 'autil::MultiChar'],
     ['DocIdFromVec', 'DocIdFromMatchDocs', 'DocIdFromMatchDocsV2'],
     ['remote_indexlib::reader::RemoteAttributeReader']],
    template_header_fill_attr,
    template_body_fill_attr2,
    template_tail,
    element_per_file=1
)
gen_cpp_code(
    'attribute_to_tensor_require_attr_generated', [((
        'int8_t', 'indexlib::index::AttributeIteratorTyped',
        'indexlib::index::AttributeIteratorBase'
    ), (
        'int8_t', 'indexlibv2::index::AttributeIteratorTyped',
        'indexlibv2::index::AttributeIteratorBase'
    ), (
        'uint8_t', 'indexlib::index::AttributeIteratorTyped',
        'indexlib::index::AttributeIteratorBase'
    ), (
        'uint8_t', 'indexlibv2::index::AttributeIteratorTyped',
        'indexlibv2::index::AttributeIteratorBase'
    ), (
        'int16_t', 'indexlib::index::AttributeIteratorTyped',
        'indexlib::index::AttributeIteratorBase'
    ), (
        'int16_t', 'indexlibv2::index::AttributeIteratorTyped',
        'indexlibv2::index::AttributeIteratorBase'
    ), (
        'uint16_t', 'indexlib::index::AttributeIteratorTyped',
        'indexlib::index::AttributeIteratorBase'
    ), (
        'uint16_t', 'indexlibv2::index::AttributeIteratorTyped',
        'indexlibv2::index::AttributeIteratorBase'
    ), (
        'int32_t', 'indexlib::index::AttributeIteratorTyped',
        'indexlib::index::AttributeIteratorBase'
    ), (
        'int32_t', 'indexlibv2::index::AttributeIteratorTyped',
        'indexlibv2::index::AttributeIteratorBase'
    ), (
        'uint32_t', 'indexlib::index::AttributeIteratorTyped',
        'indexlib::index::AttributeIteratorBase'
    ), (
        'uint32_t', 'indexlibv2::index::AttributeIteratorTyped',
        'indexlibv2::index::AttributeIteratorBase'
    ), (
        'int64_t', 'indexlib::index::AttributeIteratorTyped',
        'indexlib::index::AttributeIteratorBase'
    ), (
        'int64_t', 'indexlibv2::index::AttributeIteratorTyped',
        'indexlibv2::index::AttributeIteratorBase'
    ), (
        'uint64_t', 'indexlib::index::AttributeIteratorTyped',
        'indexlib::index::AttributeIteratorBase'
    ), (
        'uint64_t', 'indexlibv2::index::AttributeIteratorTyped',
        'indexlibv2::index::AttributeIteratorBase'
    ), (
        'float', 'indexlib::index::AttributeIteratorTyped',
        'indexlib::index::AttributeIteratorBase'
    ), (
        'float', 'indexlibv2::index::AttributeIteratorTyped',
        'indexlibv2::index::AttributeIteratorBase'
    ), (
        'double', 'indexlib::index::AttributeIteratorTyped',
        'indexlib::index::AttributeIteratorBase'
    ), (
        'double', 'indexlibv2::index::AttributeIteratorTyped',
        'indexlibv2::index::AttributeIteratorBase'
    ), (
        'char', 'indexlib::index::AttributeIteratorTyped',
        'indexlib::index::AttributeIteratorBase'
    ), (
        'char', 'indexlibv2::index::AttributeIteratorTyped',
        'indexlibv2::index::AttributeIteratorBase'
    ), (
        'autil::MultiChar', 'indexlib::index::AttributeIteratorTyped',
        'indexlib::index::AttributeIteratorBase'
    ), (
        'autil::MultiChar', 'indexlibv2::index::AttributeIteratorTyped',
        'indexlibv2::index::AttributeIteratorBase'
    ))],
    template_header_require_attr,
    template_body_require_attr,
    template_tail,
    element_per_file=1
)
