load('//aios/storage:defs.bzl', 'strict_cc_fast_test')
strict_cc_fast_test(
    name='shape_unittest',
    srcs=[
        'CircleTest.cpp', 'EventQueueTest.cpp', 'EventTest.cpp',
        'LineIntersectOperatorTest.cpp', 'LineTest.cpp', 'PointTest.cpp',
        'PolygonTest.cpp', 'RectangleIntersectOperatorTest.cpp',
        'RectangleTest.cpp', 'SLsegTest.cpp', 'ShapeCoverTest.cpp',
        'ShapeCreatorTest.cpp', 'SpatialOptimizeStrategyTest.cpp'
    ],
    copts=['-Werror', '-fno-access-control'],
    shard_count=5,
    deps=[
        '//aios/storage/indexlib/index/common:Types',
        '//aios/storage/indexlib/index/common/field_format/spatial:shape',
        '//aios/unittest_framework'
    ]
)
