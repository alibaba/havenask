package(default_visibility = ["//visibility:public"])
exports_files(["upload_package.py"])

filegroup(
    name = "gen_error_info_py",
    srcs = ["gen_error_info.py"],
    visibility = ["//visibility:public"],
)

py_binary(
    name = "install",
    srcs = ["install.py"],
    srcs_version = "PY2AND3",
    visibility = ["//visibility:public"],
    deps = [
    ],
)
