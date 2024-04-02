package(default_visibility = ["//visibility:public"])

filegroup(
    name = "shared_files",
    srcs = glob([
        "lib64/*.a",
        "lib/*.a",
        "lib/*.so",
        "lib/*.la",
        "lib/*.exp",
        "lib/libmxml.so.1",
        "lib/liboss_c_sdk.so.3.0.0",
        "lib/libaprutil-1.so.0.6.1",
        "lib/libapr-1.so.0",
        "lib/libapr-1.so.0.6.5",
        "lib/libaprutil-1.so.0",
        "lib/libaprutil-1.so.0.6.1",
    ]),
    visibility = ["//visibility:public"],
)

cc_library(
    name = "oss-shared",
    srcs = [
        ":shared_files",
        "@curl//:curl",
    ],
    linkstatic=1,
)

cc_library(
    name = "oss-cpp-sdk",
    srcs = [
        ":shared_files",
        "@curl//:curl",
    ],
    hdrs = glob([
        "include/alibabacloud/oss/*.h",
        "include/alibabacloud/oss/**/*.h",
        ]),
        
    include_prefix = "alibabacloud/oss",
    strip_include_prefix = "include/alibabacloud/oss",
    copts = [
        "-std=c++11",
        "-fno-rtti",
        "-lalibabacloud-oss-cpp-sdk",
        "-lcurl",
        "-lcrypto",
        "-lpthread",
    ],
    alwayslink = 1,
    linkstatic=1,
    visibility = ["//visibility:public"],
)

cc_library(
    name = "mxml",
    srcs = [
        ":shared_files",
    ],
    hdrs = glob([
        "include/*.h",
        ]),
    include_prefix = "",
    strip_include_prefix = "include",
    copts = [
    ],
    alwayslink = 1,
    linkstatic=1,
    visibility = ["//visibility:public"],
)

cc_library(
    name = "apr",
    srcs = [
        ":shared_files",
    ],
    hdrs = glob([
        "include/apr-1/*.h",
        ]),
    include_prefix = "",
    strip_include_prefix = "include/apr-1",
    copts = [
        "-Werror",
        "-fno-access-control",
    ],
    alwayslink = 1,
    linkstatic=1,
)

cc_library(
    name = "apr-1",
    srcs = [
        ":shared_files",
    ],
    hdrs = glob([
        "include/apr-1/*.h",
        ]),
    include_prefix = "apr-1",
    strip_include_prefix = "include/apr-1",
    copts = [
    ],
    alwayslink = 1,
    linkstatic=1,
    visibility = ["//visibility:public"],
)

cc_library(
    name = "oss-c-sdk",
    srcs = [
        ":shared_files",
    ],
    deps = [
        ":apr",
        ":mxml",
    ],
    hdrs = glob([
        "include/oss_c_sdk/*.h",
        "include/oss_c_sdk/**/*.h",
        ]),
    include_prefix = "oss_c_sdk",
    strip_include_prefix = "include/oss_c_sdk",
    copts = [
    ],
    alwayslink = 1,
    linkstatic=1,
    visibility = ["//visibility:public"],
)
