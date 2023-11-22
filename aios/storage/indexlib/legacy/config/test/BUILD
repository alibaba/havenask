load('//aios/storage:defs.bzl', 'strict_cc_fast_test', 'strict_cc_library')
strict_cc_library(
    name='schema_maker',
    srcs=[
        'modify_schema_maker.cpp', 'region_schema_maker.cpp', 'schema_maker.cpp'
    ],
    hdrs=['modify_schema_maker.h', 'region_schema_maker.h', 'schema_maker.h'],
    include_prefix='indexlib/config/test',
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[
        '//aios/storage/indexlib/framework:tablet_schema_loader',
        '//aios/storage/indexlib/legacy/config:schema'
    ]
)
strict_cc_library(
    name='legacy_config_test_helper',
    srcs=[],
    hdrs=[],
    include_prefix='indexlib/config/test',
    deps=['//aios/storage/indexlib/legacy/config:schema']
)
strict_cc_fast_test(
    name='legacy_config_unittest',
    srcs=['schema_maker_unittest.cpp'],
    copts=['-fno-access-control'],
    data=(['//aios/storage/indexlib:testdata'] + glob(['testdata/**'])),
    deps=[
        ':legacy_config_test_helper', ':schema_maker',
        '//aios/storage/indexlib/legacy/config:schema',
        '//aios/storage/indexlib/util/testutil:unittest'
    ]
)
