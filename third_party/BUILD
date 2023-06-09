package(default_visibility = ["//visibility:public"])
exports_files(["zlib.BUILD"])
load("//bazel:bundle.bzl", "bundle_files",)

bundle_files(
    name = "bhclient_package",
    srcs = [
        "@bhclient_cpp//:libbhclient_so",
    ],
    prefix = "usr/local/lib64",
    strip_prefix = "bhclient/lib",
)

bundle_files(
    name = "alinlp_package",
    srcs = [
        "//third_party/alinlp:alinlp_shared",
    ],
    prefix = "",
)

bundle_files(
    name = "dadi-cache-sdk_package",
    srcs = [
        "@dadi-cache-sdk//:dadi-cache-sdk-so",
    ],
    prefix = "",
)

bundle_files(
    name = "libnpusmi_package",
    srcs = [
        "@libnpurt//:libnpusmi_bundle",
    ],
    prefix = "usr/local/lib64",
    strip_prefix = "usr/local/lib64",
)

bundle_files(
    name = "libnpurt_package",
    srcs = [
        "@libnpurt//:libnpurt_bundle",
    ]
)