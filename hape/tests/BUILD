load("//bazel:bundle.bzl", "bundle_unpack", "bundle_tar", "bundle_files")


bundle_files(
    name = "zookeeper_server_package",
    srcs = ["//aios/filesystem/fslib/fslib/fs/zookeeper:zookeeper_server"],
    strip_prefix = "test/testdata"
)

bundle_unpack(
    name = "zookeeper_server",
    prefix = "install_root/usr/local/share",
    srcs = [":zookeeper_server_package"]
)

bundle_unpack(
    name = "aios_open_source_sql_install",
    prefix = "install_root",
    srcs = ["//package/havenask/sql:aios_open_source_sql"]
)

py_library(
    name = "test_base",
    srcs = glob([
        "hape_test_base.py"
    ]),
    deps = [
        "//hape:hape_libs"
    ],
    data = [
        ":zookeeper_server",
        ":aios_open_source_sql_install",
        "//hape:hape_conf",
        "//hape:testdata_sut"
    ]
)

py_test(
    name = "sut_swift",
    srcs = ["sut_swift.py"],
    deps = [
        ":test_base"
    ],
    tags = ["hape-test"]
)

py_test(
    name = "sut_catalog_validate",
    srcs = ["sut_catalog_validate.py"],
    deps = [
        ":test_base"
    ],
    data = [
        "//hape:testdata_sut"
    ],
    tags = ["hape-test"]
)


py_test(
    name = "sut_table_catalog",
    srcs = ["sut_table_catalog.py"],
    deps = [
        ":test_base"
    ],
    data = [
        "//hape:testdata_sut"
    ],
    tags = ["hape-test"]
)

py_test(
    name = "sut_pack_text_index",
    srcs = ["sut_pack_text_index.py"],
    deps = [
        ":test_base"
    ],
    data = [
        "//hape:testdata_sut"
    ],
    tags = ["hape-test"]
)

py_test(
    name = "sut_vector_index",
    srcs = ["sut_vector_index.py"],
    deps = [
        ":test_base"
    ],
    data = [
        "//hape:testdata_sut"
    ],
    tags = ["hape-test"]
)