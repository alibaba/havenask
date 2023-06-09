load("@bazel_skylib//lib:selects.bzl", "selects")
load("//aios/storage/indexlib:defs.bzl", "indexlib_cc_library")
load("//aios/storage/indexlib:defs.bzl", "indexlib_cc_fast_test")


def proxima2_liba_deps():
    return select({
        "//aios/storage/indexlib/index/ann/aitheta2:ppu_mode": ["//third_party/proxima2:proxima2_cpu_dev"],
        "//aios/storage/indexlib/index/ann/aitheta2:gpu_mode": ["//third_party/proxima2:proxima2_gpu"],
        "//aios/storage/indexlib/index/ann/aitheta2:new_abi_gpu_mode": ["//third_party/proxima2:proxima2_gpu_c11abi"],
        "//aios/storage/indexlib/index/ann/aitheta2:cpu_mode": ["//third_party/proxima2:proxima2_cpu"],
        "//aios/storage/indexlib/index/ann/aitheta2:new_abi_cpu_mode": ["//third_party/proxima2:proxima2_cpu_c11abi"],
        "//conditions:default": [],
    }) + select({
        "//aios/storage/indexlib/index/ann/aitheta2:gpu_mode": [
            "@local_config_cuda//cuda:cudart",
            "@local_config_cuda//cuda:cublas",
        ],
        "//conditions:default": [],
    })

def proxima2_libso_deps():
    return select({
        "//aios/storage/indexlib/index/ann/aitheta2:cpu_mode": [
            "//third_party/proxima2:proxima2_cpu",
            "//third_party/proxima2:proxima2_cpu_import",
        ],
        "//aios/storage/indexlib/index/ann/aitheta2:new_abi_cpu_mode": [
            "//third_party/proxima2:proxima2_cpu_c11abi",
            "//third_party/proxima2:proxima2_cpu_c11abi_import",
        ],
        "//aios/storage/indexlib/index/ann/aitheta2:gpu_mode": [
            "//third_party/proxima2:proxima2_gpu",
            "//third_party/proxima2:proxima2_gpu_import",
        ],
        "//aios/storage/indexlib/index/ann/aitheta2:new_abi_gpu_mode": [
            "//third_party/proxima2:proxima2_gpu_c11abi",
            "//third_party/proxima2:proxima2_gpu_c11abi_import",
        ],
        "//conditions:default": [
            "//third_party/proxima2:proxima2_cpu",
            "//third_party/proxima2:proxima2_cpu_import",
        ],
    })

def aitheta_cc_library(name, srcs = None, hdrs = None, copts = [], linkopts = [], deps = [], **kwargs):
    aitheta_liba_deps = proxima2_liba_deps()
    indexlib_cc_library(
        name = name,
        srcs = native.glob([name + ".cpp"]) if srcs == None else srcs,
        hdrs = native.glob([name + ".h"]) if hdrs == None else hdrs,
        copts = copts,
        linkopts = ["-Bsymbolic"] + linkopts,
        deps = deps + aitheta_liba_deps,
        **kwargs
    )


def aitheta_cc_test(copts = [], linkopts = [], deps = [], **kwargs):
    aitheta_liba_deps = proxima2_liba_deps()
    indexlib_cc_fast_test(
        copts = copts,
        linkopts = ["-Bsymbolic"],
        deps = deps + aitheta_liba_deps,
        **kwargs
    )
