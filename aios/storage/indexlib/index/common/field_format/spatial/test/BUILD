load('//aios/storage:defs.bzl', 'strict_cc_fast_test')
strict_cc_fast_test(
    name='spatial_unittest',
    srcs=[
        'CellTest.cpp', 'DistanceUtilTest.cpp',
        'LocationIndexQueryStrategyTest.cpp', 'QueryStrategyTest.cpp',
        'ShapeIndexQueryStrategyTest.cpp', 'SpatialFieldEncoderTest.cpp'
    ],
    copts=['-Werror', '-fno-access-control'],
    deps=[
        '//aios/storage/indexlib/index/common:Types',
        '//aios/storage/indexlib/index/common/field_format/spatial:shape',
        '//aios/unittest_framework'
    ]
)
