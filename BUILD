load(
    '@bazel_tools//tools/jdk:default_java_toolchain.bzl',
    'default_java_toolchain', 'VANILLA_TOOLCHAIN_CONFIGURATION'
)
load('@bazel_skylib//rules:common_settings.bzl', 'bool_flag')
default_java_toolchain(
    name='toolchain',
    bootclasspath=[],
    configuration=VANILLA_TOOLCHAIN_CONFIGURATION,
    javabuilder=['@remote_java_tools_8//:VanillaJavaBuilder'],
    jvm_opts=[],
    visibility=['//visibility:public'],
    java_runtime='@local_jdk//:jdk',
    tools=[]
)
config_setting(name='arm_cpu', values={'cpu': 'aarch64'})
config_setting(name='clang_build', values={'define': 'compiler_type=clang++'})
config_setting(name='enable_rdma', values={'copt': '-DUSE_RDMA=1'})
config_setting(name='enable_solar', values={'copt': '-DACCL_USE_SOLAR=1'})
config_setting(
    name='new_abi_mode',
    define_values={'use_abi': '1'},
    visibility=['//visibility:public']
)
config_setting(
    name='old_abi_mode',
    define_values={'use_abi': '0'},
    visibility=['//visibility:public']
)
config_setting(
    name='npu', values={'copt': '-DNPU=1'}, visibility=['//visibility:public']
)
config_setting(
    name='fslib_enable_async',
    define_values={'fslib_use_async': 'yes'},
    visibility=['//visibility:public']
)
config_setting(
    name='fslib_disable_async',
    define_values={'fslib_use_async': 'no'},
    visibility=['//visibility:public']
)
config_setting(
    name='build_with_aicompiler',
    define_values={'build_with_aicompiler': 'true'},
    visibility=['//visibility:public']
)
config_setting(
    name='build_with_ppu',
    define_values={'build_with_ppu': 'true'},
    visibility=['//visibility:public']
)
config_setting(
    name='remote_exec',
    define_values={'with_remote_exec': 'true'},
    visibility=['//visibility:public']
)
config_setting(
    name='aios_open_source',
    define_values={'repo_mode': 'aios_open_source'},
    visibility=['//visibility:public']
)
config_setting(
    name='cpu_with_avx512',
    define_values={'cpu_avx512': 'true'},
    visibility=['//visibility:public']
)
config_setting(
    name='cpu_without_avx512',
    define_values={'cpu_avx512': 'false'},
    visibility=['//visibility:public']
)
config_setting(
    name='hack_get_set_env',
    define_values={'hack_get_set_env': 'true'},
    visibility=['//visibility:public']
)
bool_flag(name='using_cuda', build_setting_default=False)
bool_flag(name='using_ppu', build_setting_default=False)
config_setting(
    name='ppu_mode',
    flag_values={
        ':using_cuda': 'True',
        ':using_ppu': 'True'
    },
    visibility=['//visibility:public']
)
config_setting(
    name='gpu_mode',
    flag_values={
        ':using_cuda': 'True',
        ':using_ppu': 'False'
    },
    visibility=['//visibility:public']
)
config_setting(
    name='cpu_mode',
    flag_values={
        ':using_cuda': 'False',
        ':using_ppu': 'False'
    },
    visibility=['//visibility:public']
)

config_setting(
    name='disable_catalog_mongodb',
    values={'define': 'disable_catalog_mongodb=true'},
    visibility=['//visibility:public']
)
load(
    '@io_bazel_rules_docker//container:container.bzl', 'container_layer',
    'container_image'
)
container_layer(
    name='python_symlink',
    symlinks={
        '/usr/bin/python': '/usr/local/bin/python',
        '/usr/bin/python3': '/usr/local/bin/python'
    }
)
container_image(
    name='python3',
    base='@python_container//image',
    layers=[':python_symlink'],
    visibility=['//visibility:public']
)
