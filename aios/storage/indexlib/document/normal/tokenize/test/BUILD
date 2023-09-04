load('//bazel:defs.bzl', 'cc_fast_test')
cc_fast_test(
    name='indexlib_tokenize_unittest',
    srcs=glob(['*Test.cpp']),
    copts=['-Werror', '-fno-access-control'],
    data=['//aios/storage/indexlib:testdata'],
    deps=[
        '//aios/autil:thread',
        '//aios/storage/indexlib/document/normal/tokenize',
        '//aios/storage/indexlib/util/testutil:unittest'
    ]
)
