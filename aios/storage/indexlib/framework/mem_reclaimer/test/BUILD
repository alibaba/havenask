load('//aios/storage:defs.bzl', 'strict_cc_fast_test')
strict_cc_fast_test(
    name='EpochBasedMemReclaimerTest',
    srcs=['EpochBasedMemReclaimerTest.cpp'],
    copts=['-fno-access-control'],
    data=[],
    deps=[
        '//aios/autil:thread', '//aios/future_lite',
        '//aios/storage/indexlib/framework/mem_reclaimer:EpochBasedMemReclaimer',
        '//aios/storage/indexlib/util/testutil:unittest'
    ]
)
