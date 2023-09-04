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
    srcs = ["usr/local/lib/arch_skylake-avx512/libailego.lo",
            "usr/local/lib/arch_skylake-avx512/libaitheta2.lo",
            "usr/local/lib/arch_skylake-avx512/libknn_cluster_cc.lo",
            "usr/local/lib/arch_skylake-avx512/libknn_hc_cc.lo",
            "usr/local/lib/arch_skylake-avx512/libknn_oswg_cc.lo",
            "usr/local/lib/arch_skylake-avx512/libknn_hnsw_cc.lo",
            "usr/local/lib/arch_skylake-avx512/libknn_qc_cc.lo"],
    hdrs = [],
    deps = [],
    alwayslink = 1,
    visibility = ["//visibility:public"],
)
cc_library(
    name = "proxima2_gpu",
    srcs = ["usr/local/lib/arch_skylake-avx512/libknn_gpu_qc_cc.lo",
            "usr/local/lib/arch_skylake-avx512/libgpu_utils.lo",
            "usr/local/lib/arch_skylake-avx512/libgpu_impl.lo",
            "usr/local/lib/arch_skylake-avx512/libqc_internal_searcher.lo"],
    hdrs = [],
    deps = [":proxima2_cpu"],
    alwayslink = 1,
    visibility = ["//visibility:public"],
)
cc_library(
    name = "proxima2_cpu_c11abi",
    srcs = ["usr/local/lib/c11abi/arch_skylake-avx512/libailego.lo",
            "usr/local/lib/c11abi/arch_skylake-avx512/libaitheta2.lo",
            "usr/local/lib/c11abi/arch_skylake-avx512/libknn_cluster_cc.lo",
            "usr/local/lib/c11abi/arch_skylake-avx512/libknn_hc_cc.lo",
            "usr/local/lib/c11abi/arch_skylake-avx512/libknn_oswg_cc.lo",
            "usr/local/lib/c11abi/arch_skylake-avx512/libknn_hnsw_cc.lo",
            "usr/local/lib/c11abi/arch_skylake-avx512/libknn_qc_cc.lo"],
    hdrs = [],
    deps = [],
    alwayslink = 1,
    visibility = ["//visibility:public"],
)
cc_library(
    name = "proxima2_gpu_c11abi",
    srcs = ["usr/local/lib/c11abi/arch_skylake-avx512/libknn_gpu_qc_cc.lo",
            "usr/local/lib/c11abi/arch_skylake-avx512/libgpu_utils.lo",
            "usr/local/lib/c11abi/arch_skylake-avx512/libgpu_impl.lo",
            "usr/local/lib/c11abi/arch_skylake-avx512/libqc_internal_searcher.lo"],
    hdrs = [],
    deps = [":proxima2_cpu_c11abi"],
    alwayslink = 1,
    visibility = ["//visibility:public"],
)