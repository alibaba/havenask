package(default_visibility = ["//hape:__subpackages__"])
load("@rules_python//experimental/python:wheel.bzl", "py_package", "py_wheel")
load("//bazel:bundle.bzl", "bundle_files", "bundle_install", "bundle_unpack", "bundle_tar")
load("@rules_python//python:pip.bzl", "pip_install")


load("@hape_depends//:requirements.bzl", "requirement")

filegroup(
    name = "hape_conf",
    srcs = glob([
        "hape_conf/default/**",
        "hape_conf/remote/**",
    ])
)

filegroup(
    name = "sql_query",
    srcs = [
        "hape_libs/utils/sql_query.py"
    ]
)

filegroup(
    name = "swift_writer",
    srcs = [
        "hape_libs/utils/swift_writer.py"
    ]
)

filegroup(
    name = "hape_example",
    srcs = glob([
        "example/**"
    ])
)

filegroup(
    name = "hape_bin",
    srcs = ["hape"]
)


py_library(
    name = "hape_libs",
    srcs = glob([
        "hape_libs/**/*.py",
        "hape_libs/testlib/*.py"
    ]),
    imports = [
        "../aios/network/arpc"
    ],
    deps = [
        requirement("kazoo"),
        requirement("snakebite"),
        requirement("requests"),
        requirement("selectors2"),
        requirement("click"),
        "//aios/suez/python:builder",
        "//aios/network/arpc:arpc_python"
    ]
)

filegroup(
    name = "py_init_file",
    srcs = ["hape_libs/__init__.py"]
)


bundle_files(
    name = "aios_init_py",
    srcs = [
        ":py_init_file"
    ],
    strip_prefix = "hape_libs",
    prefix = "usr/local/lib/python/site-packages/aios"
)

bundle_files(
    name = "suez_init_py",
    srcs = [
        ":py_init_file"
    ],
    deps = [
        ":aios_init_py"
    ],
    strip_prefix = "hape_libs",
    prefix = "usr/local/lib/python/site-packages/aios/suez"
)

bundle_files(
    name = "suez_admin_init_py",
    srcs = [
        ":py_init_file"
    ],
    deps = [
        ":suez_init_py"
    ],
    strip_prefix = "hape_libs",
    prefix = "usr/local/lib/python/site-packages/aios/suez/admin"
)

bundle_files(
    name = "catalog_init_py",
    srcs = [
        ":py_init_file"
    ],
    deps = [
        ":aios_init_py"
    ],
    strip_prefix = "hape_libs",
    prefix = "usr/local/lib/python/site-packages/aios/catalog"
)

bundle_files(
    name = "catalog_proto_init_py",
    srcs = [
        ":py_init_file"
    ],
    deps = [
        ":catalog_init_py"
    ],
    strip_prefix = "hape_libs",
    prefix = "usr/local/lib/python/site-packages/aios/catalog/proto"
)

bundle_files(
    name = "suez_py2_init",
    srcs = [
        ":py_init_file"
    ],
    strip_prefix = "hape_libs",
    prefix = "usr/local/lib/python/site-packages/aios/suez/python"
)

bundle_files(
    name = "suez_sdk_py2_init",
    srcs = [
    ],
    deps = [
        ":suez_py2_init",
        ":catalog_proto_init_py",
        ":suez_admin_init_py",
    ]
)

py_package(
    name = "hape_libs_py_package",
    deps = [
        ":hape_libs"
    ],
)

bundle_files(
    name = "hape_libs_py_package_files",
    srcs = [
        ":hape_libs_py_package"
    ],
    deps = [
    ],
    prefix = "usr/local/lib/python/site-packages",
    visibility = ["//visibility:public"]
)

bundle_files(
    name = "zookeeper_server_package",
    srcs = ["//aios/filesystem/fslib/fslib/fs/zookeeper:zookeeper_server"],
    strip_prefix = "test/testdata",
    prefix = "usr/local/share"
)

bundle_files(
    name = "sql_query_file",
    srcs = [
        ":sql_query"
    ],
    strip_prefix = "hape_libs/utils"
)

bundle_files(
    name = "swift_writer_file",
    srcs = [
        ":swift_writer"
    ],
    strip_prefix = "hape_libs/utils"
)

bundle_files(
    name = "hape_files",
    srcs = [
        ":hape_conf",
        ":hape_bin",
        ":hape_example",
    ],
    deps = [
        "//aios/suez/python:suez_py_files",
        ":suez_sdk_py2_init",
        ":zookeeper_server_package",
        ":hape_libs_py_package_files",
        "//package/havenask/sql:aios_open_source_sql",
        ":sql_query_file",
        ":swift_writer_file"
    ],
    visibility = ["//visibility:public"]
)

filegroup(
    name= "testdata_sut",
    srcs = glob([
        "testdata/**/**",
        "testdata/*"
    ])
)