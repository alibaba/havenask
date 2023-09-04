package(default_visibility = ["//visibility:public"])

load("@com_taobao_aios//bazel:defs.bzl", "cc_proto")

arm_cpu = "@com_taobao_aios//:arm_cpu"

config_setting(
    name = "arm_cpu",
    values = {"cpu": "aarch64"},
)

cc_proto(
    name = "bhclient",
    srcs = glob([
        "bhclient/proto/blackhole_client_config.proto",
        "bhclient/proto/schema.proto",
    ]),
    deps = [
    ],
    visibility = ["//visibility:public"],
)

cc_library(
    name = "bhclient_headers",
    hdrs = glob([
        "bhclient/include/blackhole_client_c.h",
    ]),
    strip_include_prefix = "bhclient/include/",
    visibility = ["//visibility:public"],
)

cc_library(
    name = "binaryrow_headers",
    hdrs = glob([
        "bhclient/include/binaryrow/binaryrow_rw.h",
        "bhclient/include/binaryrow/binaryrow.h",
        "bhclient/include/binaryrow/binaryrow_set.h",
    ]),
    strip_include_prefix = "bhclient/include/",
    visibility = ["//visibility:public"],
)

filegroup(
    name= "libbhclient_so",
    srcs = select({
        arm_cpu: glob(["bhclient/lib/aarch64/libbhclient_dlopen.so"]),
        "//conditions:default": glob(["bhclient/lib/libbhclient_dlopen.so"]),
    }),
    visibility = ["//visibility:public"],
)

cc_library(
    name = "bhclient_cpp",
    srcs = select({
        arm_cpu: glob(["bhclient/lib/aarch64/libbhclient_dlopen.so"]),
        "//conditions:default": glob(["bhclient/lib/libbhclient_dlopen.so"]),
    }),
    deps = [
        ":bhclient_headers",
        ":binaryrow_headers",
        ":bhclient_cc_proto",
    ],
    visibility = ["//visibility:public"],
    linkstatic = 1,
)
