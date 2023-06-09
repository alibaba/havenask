package(default_visibility = ["//visibility:public"])

cc_library(
    name = "hdfs_headers",
    hdrs = glob([
        "src/include/hdfs/hdfs.h",
        "src/include/hadoop/*.hh",
    ]),
    srcs = [],
    strip_include_prefix = "src/include",
    visibility = ["//visibility:public"],
)

cc_library(
    name = "hdfs",
    srcs = glob([
        "src/lib64/*.so",
        "src/lib64/*.so.*",
    ]),
    deps = [
        ":hdfs_headers",
    ],
    visibility = ["//visibility:public"],
)

filegroup(
    name = "hdfs_shared",
    srcs = glob([
        #"src/lib64/*.so",
        "src/lib64/*.so.*",
    ]),
    visibility = ["//visibility:public"],
)
