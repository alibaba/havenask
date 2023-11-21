load('//aios/storage:defs.bzl', 'strict_cc_fast_test', 'strict_cc_library')
strict_cc_fast_test(
    name='indexlib_cache_unittest',
    srcs=['MemoryCacheTest.cpp', 'SearchCacheCreatorTest.cpp'],
    copts=['-fno-access-control'],
    deps=[
        '//aios/storage/indexlib/util/cache',
        '//aios/storage/indexlib/util/testutil:unittest'
    ]
)
strict_cc_library(
    name='fake_dadi',
    testonly=True,
    srcs=['FakeDadi.cpp'],
    hdrs=[],
    tags=['not_for_opensource'],
    visibility=[
        '//aios/storage/indexlib/indexlib/index/normal/attribute/test:__subpackages__'
    ],
    deps=[
        '//aios/autil:lock', '//aios/autil:thread',
        '//aios/filesystem/fslib:fslib-framework',
        '@dadi-cache-sdk//:dadi-cache-sdk'
    ]
)
