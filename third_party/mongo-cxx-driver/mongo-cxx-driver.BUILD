package(default_visibility = ["//visibility:public"])

filegroup(
    name = "shared_files",
    srcs = [
        "lib64/libbson-1.0.so",
        "lib64/libbson-1.0.so.0",
        "lib64/libbson-1.0.so.0.0.0",
        "lib64/libbsoncxx.so",
        "lib64/libbsoncxx.so.3.6.5",
        "lib64/libbsoncxx.so._noabi.so",
        "lib64/libmongoc-1.0.so",
        "lib64/libmongoc-1.0.so.0",
        "lib64/libmongoc-1.0.so.0.0.0",
        "lib64/libmongocxx.so",
        "lib64/libmongocxx.so.3.6.5",
        "lib64/libmongocxx.so._noabi.so",
    ],
    visibility = ["//visibility:public"],
)

# "usr/include/libmongoc-1.0/*",
# "usr/include/libbson-1.0/*",

cc_library(
    name = "mongo-bsoncxx",
    srcs = [
        ":shared_files",
    ],
    hdrs = glob([
        "include/bsoncxx/v_noabi/bsoncxx/**/*.hpp",
        ]),
    include_prefix = "bsoncxx",
    strip_include_prefix = "include/bsoncxx/v_noabi/bsoncxx",
    copts = [
        "-std=c++17"
    ],
    alwayslink = 1,
    linkstatic=1,
    visibility = ["//visibility:public"],
)

cc_library(
    name = "mongo-cxx-driver",
    deps = [
        ":mongo-bsoncxx",
    ],
    srcs = [
        ":shared_files",
    ],
    hdrs = glob([
        "include/mongocxx/v_noabi/mongocxx/**/*.hpp",
        ]),
        
    include_prefix = "mongocxx",
    strip_include_prefix = "include/mongocxx/v_noabi/mongocxx",
    copts = [
        "-std=c++17"
    ],
    alwayslink = 1,
    linkstatic=1,
    visibility = ["//visibility:public"],
)
