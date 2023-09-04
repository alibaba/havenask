cc_library(
    name='mock_file_deployer',
    testonly=True,
    hdrs=['MockFileDeployer.h'],
    include_prefix='suez/deploy/test',
    visibility=['//aios/suez:__subpackages__']
)
cc_test(
    name='suez_deploy_test',
    srcs=glob(['*.cpp', '*.h'], exclude=['MockFileDeployer.h']),
    copts=['-fno-access-control'],
    data=['//aios/suez:testdata'],
    deps=[
        ':mock_file_deployer', '//aios/suez/deploy', '//aios/unittest_framework'
    ]
)
