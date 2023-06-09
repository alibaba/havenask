package(default_visibility = ["//visibility:public"])

load("@com_taobao_aios//bazel:defs.bzl", "cc_proto", "gen_conf_lib")

cc_proto(
    name = "orc",
    srcs = ["proto/orc_proto.proto"],
    deps = []
)

gen_conf_lib(
    name = "config_h",
    srcs = ["cpp/include/orc/orc-config.hh.in"],
    outs = ["cpp/include/orc/orc-config.hh"],
    args = {
        "@ORC_VERSION@" : "1.0.0",
        "cmakedefine" : "define"
    },
    strip_include_prefix = "cpp/include",
)

gen_conf_lib(
    name = "adaptor_h",
    srcs = ["cpp/include/orc/Adaptor.hh.in"],
    outs = ["cpp/include/orc/Adaptor.hh"],
    args = {
        "#cmakedefine INT64_IS_LL" : "",
        "#cmakedefine HAS_POST_2038" : "",
        "#cmakedefine NEEDS_REDUNDANT_MOVE" : "",
        "#define GTEST_LANG_CXX11 0" : "",
        "cmakedefine" : "define"
    },
    strip_include_prefix = "cpp/include",
)

cc_library(
    name = "roaring_bitmap_h",
    hdrs = [
         "cpp/src/roaring/include/roaring_bitmap.h"
    ],
    strip_include_prefix = "cpp/src/roaring"
)

cc_library(
    name = "aliorc_c",
    srcs = glob([
         "cpp/src/roaring/src/roaring/roaring.h",
         "cpp/src/roaring/src/roaring/roaring.hh",
         "cpp/src/roaring/src/roaring/roaring.c"]
    )
)

cc_library(
    name = "aliorc",
    hdrs = glob([
        "cpp/include/orc/*.hh",
        "cpp/include/orc/sargs/*.hh",
        "cpp/include/wrap/*.hh",
    ]),
    srcs = glob([
        "cpp/src/*.cc",
        "cpp/src/*.hh",
        "cpp/src/encoding/*.hh",
        "cpp/src/encoding/*.cc",
        "cpp/src/encoding/FastPFor/*.h",
        "cpp/src/encoding/FastPFor/*.cpp",
        "cpp/src/encoding/Dictionary/**",
        "cpp/src/encoding/RLEv2/**",
        "cpp/src/sargs/**",
        "cpp/src/wrap/**",
        "cpp/libs/sparsehash/sparsehash/*.h",
        "cpp/libs/sparsehash/sparsehash/internal/*.h",
        "cpp/src/roaring/src/*.h",
        "cpp/src/roaring/src/*.cpp",

    ], exclude = [
        "cpp/src/C09Adapter.cc",
        "cpp/src/wrap/gmock.h",
        "cpp/src/wrap/gtest-wrapper.h",
        "cpp/src/wrap/orc-proto-wrapper.cc",
    ]),
    includes = [
        "proto/",
        "cpp/src/",
        "cpp/src/encoding/",
        "cpp/src/encoding/RLEv2",
        "cpp/libs/sparsehash",
    ],
    deps = [
        ":aliorc_c",
        ":config_h",
        ":adaptor_h",
        ":roaring_bitmap_h",
        ":orc_cc_proto",
        "@zlib_archive//:zlib",
        "@snappy",
        "@com_taobao_aios//third_party/lz4",
        "@com_taobao_aios//third_party/zstd",
    ],
    copts = [
        "-Wno-unused-private-field",
        "-include stdexcept",
        "-Wno-class-memaccess",
        "-Wno-deprecated-declarations"
    ],
    strip_include_prefix = "cpp/include",
    alwayslink = 1,
)

cc_binary(
    name = "orc_metadata",
    srcs = [
        "tools/src/FileMetadata.cc",
    ],
    deps = [
        ":aliorc",
    ],
)
