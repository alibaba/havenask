package(default_visibility=['//aios/suez:__subpackages__'])
cc_library(
    name='deploy',
    srcs=[
        'DeployItem.cpp', 'DeployManager.cpp', 'FileDeployer.cpp',
        'IndexChecker.cpp', 'IndexDeployer.cpp', 'LocalDeployItem.cpp',
        'NormalDeployItem.cpp'
    ],
    hdrs=[
        'DeployFiles.h', 'DeployItem.h', 'DeployManager.h', 'FileDeployer.h',
        'IndexChecker.h', 'IndexDeployer.h', 'LocalDeployItem.h',
        'NormalDeployItem.h'
    ],
    include_prefix='suez/deploy',
    deps=[
        '//aios/storage/indexlib/config:options',
        '//aios/storage/indexlib/framework:tablet', '//aios/suez/common',
        '//aios/suez/sdk:utils', '//aios/worker_framework'
    ]
)
