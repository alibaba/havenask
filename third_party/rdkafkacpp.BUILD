package(default_visibility = ["//visibility:public"])

cc_library(
    name = "rdkafkacpp",
    visibility = ["//visibility:public"],
    srcs = glob([
        "rdkafka_2_0_2_libs/lib/*.so",
        "rdkafka_2_0_2_libs/lib/*.so.1",
    ]),
    hdrs = glob([
         "rdkafka_2_0_2_libs/include/**/*.h",
    ]),
    strip_include_prefix = "rdkafka_2_0_2_libs/include",
)

filegroup(
    name = "rdkafkacpp_shared",
    srcs = glob([
        "rdkafka_2_0_2_libs/lib/*.so",
        "rdkafka_2_0_2_libs/lib/*.so.1",
    ]),
    visibility = ["//visibility:public"],
)
