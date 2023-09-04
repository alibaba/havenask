package(default_visibility = ["//aios/suez:__subpackages__"])

load("//bazel:bundle.bzl", "bundle_files")

py_library(
    name = "builder",
    srcs = glob([
        "base_builder.py",
        "cluster_builder.py",
        "catalog_builder.py",
    ]),
    visibility = ["//visibility:public"],
    deps = [
        "//aios/catalog/proto:catalog_entity_py_proto",
        "//aios/suez/admin:cluster_service_py_proto",
    ],
)

bundle_files(
    name = "suez_py_files",
    srcs = [
        ":builder",
    ],
    prefix = "usr/local/lib/python/site-packages/aios/suez/python",
    visibility = ["//visibility:public"],
    deps = [
        ":catalog_proto_py",
        ":cluster_proto_py",
    ],
)

bundle_files(
    name = "cluster_proto_py",
    srcs = [
        "//aios/suez/admin:cluster_service_py_proto",
    ],
    prefix = "usr/local/lib/python/site-packages/aios/suez/admin",
    visibility = ["//visibility:public"],
)

bundle_files(
    name = "catalog_proto_py",
    srcs = [
        "//aios/catalog/proto:catalog_entity_py_proto",
    ],
    prefix = "usr/local/lib/python/site-packages/aios/catalog/proto",
    visibility = ["//visibility:public"],
)
