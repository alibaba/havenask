package(default_visibility = ["//visibility:public"])
# include
cc_library(
    name = "proxima2_headers",
    srcs = [],
    hdrs = glob(["usr/local/include/**"]),
    include_prefix = "",
    strip_include_prefix = "usr/local/include",
    visibility = ["//visibility:public"],
)

cc_library(
    name = "proxima2_cpu",
    srcs = glob(["usr/local/lib/arch_skylake/**"]),
    hdrs = [],
    deps = [],
    alwayslink = 1,
    visibility = ["//visibility:public"],
)

cc_library(
    name = "proxima2_gpu",
    srcs = glob(["usr/local/lib/arch_skylake-avx512/**"]),
    hdrs = [],
    deps = [],
    alwayslink = 1,
    visibility = ["//visibility:public"],
)

cc_library(
    name = "proxima2_cpu_c11abi",
    srcs =  glob(["usr/local/lib/c11abi/arch_skylake/**"]),
    hdrs = [],
    deps = [],
    alwayslink = 1,
    visibility = ["//visibility:public"],
)