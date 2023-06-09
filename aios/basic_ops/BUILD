package(
    default_visibility=[
        '//aios/suez_turing/suez/turing/python:__subpackages__',
        '//aios/basic_ops:__subpackages__'
    ]
)
load('//bazel:defs.bzl', 'cc_proto')
load('//bazel:defs.bzl', 'copy_target_to', 'gen_cpp_code')
load('//bazel:defs.bzl', 'genlex', 'genyacc', 'test_cc_plugin_shared_library')
load('@flatbuffers//:build_defs.bzl', 'flatbuffer_library_public')
load('@org_tensorflow//tensorflow:tensorflow.bzl', 'tf_custom_op_library')
load('//aios/basic_ops:basic_ops.bzl', 'register_custom_op')
cc_library(
    name='rapid_json_common',
    hdrs=['basic_ops/common/RapidJsonCommon.h'],
    include_prefix='basic_ops',
    strip_include_prefix='basic_ops',
    visibility=['//visibility:public'],
    deps=['//aios/autil:json_helper']
)
cc_library(
    name='qinfo_variant',
    srcs=['basic_ops/variant/QInfoFieldVariant.cpp'],
    hdrs=['basic_ops/variant/QInfoFieldVariant.h'],
    implementation_deps=[
        '@suez_turing.turbojet.turing_ops_util//:turing_ops_util'
    ],
    include_prefix='basic_ops',
    strip_include_prefix='basic_ops',
    visibility=['//visibility:public'],
    deps=[':rapid_json_common'],
    alwayslink=1
)
cc_library(
    name='basic_ops_op_util',
    srcs=[],
    hdrs=[
        'basic_ops/common/CommonConstDefine.h',
        'basic_ops/common/CommonDefine.h'
    ],
    include_prefix='basic_ops',
    strip_include_prefix='basic_ops',
    visibility=['//visibility:public'],
    deps=[
        '//aios/autil:timeout_terminator', '//aios/resource_reader',
        '@suez_turing.turbojet.turing_ops_util//:suez_turing',
        '@suez_turing.turbojet.turing_ops_util//:turing_ops_util'
    ]
)
elements = [
    ('SingleValueWeightsReader', 'float', 'KeyFormatter', 'true'),
    ('SingleValueWeightsReader', 'double', 'KeyFormatter', 'true'),
    ('MultiValueWeightsReader', 'int8_t', 'FingerPrintHashFormatter', 'true'),
    ('MultiValueWeightsReader', 'uint16_t', 'FingerPrintHashFormatter', 'true'),
    ('MultiValueWeightsReader', 'float', 'FingerPrintHashFormatter', 'true'),
    ('MultiValueWeightsReader', 'double', 'FingerPrintHashFormatter', 'true'),
    ('MultiValueWeightsReader', 'int8_t', 'StringToHash64Formatter', 'true'),
    ('MultiValueWeightsReader', 'uint16_t', 'StringToHash64Formatter', 'true'),
    ('MultiValueWeightsReader', 'float', 'StringToHash64Formatter', 'true'),
    ('MultiValueWeightsReader', 'double', 'StringToHash64Formatter', 'true'),
    ('MultiValueWeightsReader', 'int8_t', 'KeyFormatter', 'true'),
    ('MultiValueWeightsReader', 'uint16_t', 'KeyFormatter', 'true'),
    ('MultiValueWeightsReader', 'float', 'KeyFormatter', 'true'),
    ('MultiValueWeightsReader', 'double', 'KeyFormatter', 'true'),
    ('MultiValueWeightsReader', 'int8_t', 'MurmurHashFormatter<0>', 'true'),
    ('MultiValueWeightsReader', 'uint16_t', 'MurmurHashFormatter<0>', 'true'),
    ('MultiValueWeightsReader', 'float', 'MurmurHashFormatter<0>', 'true'),
    ('MultiValueWeightsReader', 'double', 'MurmurHashFormatter<0>', 'true'),
    ('MultiValueWeightsReader', 'int8_t', 'NoneHashFormatter', 'true'),
    ('MultiValueWeightsReader', 'uint16_t', 'NoneHashFormatter', 'true'),
    ('MultiValueWeightsReader', 'float', 'NoneHashFormatter', 'true'),
    ('MultiValueWeightsReader', 'double', 'NoneHashFormatter', 'true'),
    ('WeightsReaderTyped', 'float', 'NoneHashFormatter', 'false'),
    ('WeightsReaderTyped', 'float', 'KeyFormatter', 'false'),
    ('WeightsReaderTyped', 'float', 'MurmurHashFormatter<0>', 'false'),
    ('WeightsReaderTyped', 'float', 'StringToHash64Formatter', 'false'),
    ('WeightsReaderTyped', 'float', 'FingerPrintHashFormatter', 'false'),
    ('DenseStoreWeightsReader', 'float', 'FingerPrintHashFormatter', 'false'),
    (
        'DenseStoreWeightsReader', 'uint16_t', 'FingerPrintHashFormatter',
        'false'
    )
]
template_header = '\n#include "basic_ops/util/Feature2TensorLiteImpl.h"\n\nnamespace suez {\nnamespace turing {\n'
template = '\n#define NEED_ADAPTER {3}\n\n#if(NEED_ADAPTER==true)\nEXTERN_TEMPLATE_INTERNAL_WITH_ADAPTER({0}, {1}, {2});\n#else\nEXTERN_TEMPLATE_INTERNAL({0}, {1}, {2});\n#endif\n\n#undef NEED_ADAPTER\n'
template_tail = '\n}\n}\n'
gen_cpp_code(
    name='gen_feature_2_tensorflow_lite_impl',
    element_per_file=1,
    elements_list=[elements],
    template=template,
    template_header=template_header,
    template_tail=template_tail
)
template_header = '\n#include "basic_ops/util/Feature2TensorLite.h"\n\nnamespace suez {\nnamespace turing {\n'
template = '\n#define NEED_ADAPTER {3}\n\n#if(NEED_ADAPTER==true)\nEXTERN_TEMPLATE_INTERNAL_DECL_WITH_ADAPTER({0}, {1}, {2});\n#else\nEXTERN_TEMPLATE_INTERNAL_DECL({0}, {1}, {2});\n#endif\n\n#undef NEED_ADAPTER\n'
template_tail = '\n}\n}\n'
gen_cpp_code(
    name='gen_feature_2_tensorflow_lite_decl',
    element_per_file=10,
    elements_list=[elements],
    suffix='.h',
    template=template,
    template_header=template_header,
    template_tail=template_tail
)
register_custom_op(
    op_dir_path='basic_ops/ops/ha3',
    deps=['//aios/basic_ops:basic_ops_base'],
    include_prefix='basic_ops',
    strip_include_prefix='basic_ops'
)
register_custom_op(
    op_dir_path='basic_ops/ops/table_to_tensor',
    deps=[':basic_ops_base', '//aios/storage/kiwi:kiwi_common'],
    include_prefix='basic_ops',
    strip_include_prefix='basic_ops'
)
register_custom_op(
    op_dir_path='basic_ops/ops/tensor_to_query',
    deps=[':basic_ops_base', '//aios/turing_ops_util:param_converter'],
    include_prefix='basic_ops',
    strip_include_prefix='basic_ops'
)
register_custom_op(
    op_dir_path='basic_ops/ops/table_to_query',
    deps=[':basic_ops_base', '//aios/turing_ops_util:param_converter'],
    include_prefix='basic_ops',
    strip_include_prefix='basic_ops'
)
register_custom_op(
    op_dir_path='basic_ops/ops/table',
    deps=[':basic_ops_base', '//aios/storage/kiwi:kiwi_common'],
    include_prefix='basic_ops',
    strip_include_prefix='basic_ops'
)
register_custom_op(
    'basic_ops/ops/flow_control/condition_switch_op',
    deps=['//aios/basic_ops:basic_ops_base'],
    include_prefix='basic_ops',
    strip_include_prefix='basic_ops'
)
register_custom_op(
    op_dir_path='basic_ops/ops/local_debug',
    deps=['//aios/basic_ops:basic_ops_base'],
    include_prefix='basic_ops',
    strip_include_prefix='basic_ops'
)
