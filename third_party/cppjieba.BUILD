package(default_visibility = ["//visibility:public"])

cc_library(
    name = "cppjieba",
    hdrs = glob([
        "cppjieba/include/cppjieba/*.hpp",
    ]),
    deps = [":limonp"],
    strip_include_prefix = "cppjieba/include",
)

cc_library(
    name = "limonp",
    hdrs = glob([
        "limonp/include/limonp/*.hpp",
    ]),
    strip_include_prefix = "limonp/include",
)


filegroup(
    name = "cppjieba_dict",
    srcs = glob([
        "cppjieba/dict/**",
    ])
)