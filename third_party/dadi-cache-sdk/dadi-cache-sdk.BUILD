package(default_visibility = ["//visibility:public"])

cc_library(
    name = "dadi-cache-sdk",
    srcs = ["usr/local/lib/libdadi.so"],
    hdrs = ["dadi-cache/dadi.h"],
    linkstatic = True,
)

filegroup(
    name = "dadi-cache-sdk-so",
    srcs = ["usr/local/lib/libdadi.so"],
    visibility = ["//visibility:public"],
)
