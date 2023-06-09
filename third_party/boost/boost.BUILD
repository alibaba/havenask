package(default_visibility = ["//visibility:public"])

licenses(["notice"])

load("@com_taobao_aios//third_party/boost:boost.bzl", "boost_library")
load("@com_taobao_aios//third_party/boost:boost.bzl", "boost_headers")

cc_library(
    name = "headers-base",
    hdrs = glob([
        "boost/*.h",
        "boost/*.hpp",
        "boost/*.ipp",
    ]),
    include_prefix = "boost",
    includes = [
        ".",
    ],
    strip_include_prefix = "boost",
    visibility = ["//visibility:public"],
    deps = [
        ":config-headers",
        ":smart_ptr-headers",
    ],
)

cc_library(
    name = "headers-all",
    hdrs = glob([
        "boost/**/*.h",
        "boost/**/*.hpp",
        "boost/**/*.ipp",
    ]),
    include_prefix = "boost",
    # includes: list of include dirs added to the compile line
    includes = [
        ".",
    ],
    strip_include_prefix = "boost",
    visibility = ["//visibility:public"],
)

boost_headers(
    name = "integer",
)

boost_headers(
    name = "functional",
    deps = [
        ":integer-headers",
    ]
)

boost_headers(
    name = "regex",
)

boost_headers(
    name = "context",
    deps = [
        "headers-base",
    ],
)

boost_headers(
    name = "core",
)

boost_headers(
    name = "detail",
)

boost_headers(
    name = "smart_ptr",
    deps = [
        ":core-headers",
        ":detail-headers",
        ":exception-headers",
        ":predef-headers",
        ":move-headers",
    ],
)

boost_headers(
    name = "preprocessor",
)

boost_headers(
    name = "multiprecision",
)

boost_headers(
    name = "math",
)

boost_headers(
    name = "config",
)

boost_headers(
    name = "exception",
)

boost_headers(
    name = "mpl",
    deps = [
        ":preprocessor-headers",
    ],
)

boost_headers(
    name = "numeric",
)

boost_headers(
    name = "predef",
)

boost_headers(
    name = "operators",
)

boost_headers(
    name = "type_traits",
)

boost_headers(
    name = "utility",
)

boost_headers(
    name = "move",
)

boost_headers(
    name = "intrusive",
)

boost_headers(
    name = "interprocess",
    deps = [
        ":intrusive-headers",
        ":date_time-headers",
    ],
)

boost_headers(
    name = "process",
)

boost_headers(
    name = "property_tree",
)

boost_headers(
    name = "date_time",
    deps = [
        ":mpl-headers",
        ":numeric-headers",
        ":type_traits-headers",
    ],
)

boost_headers(
    name = "hana",
    deps = [
        "headers-base",
    ],
)

boost_headers(
    name = "function"
)

boost_headers(
    name = "type_index"
)

boost_headers(
    name = "bind"
)

boost_headers(
    name = "tuple"
)

boost_headers(
    name = "iterator"
)

boost_headers(
    name = "graph"
)

boost_headers(
    name = "range"
)

boost_headers(
    name = "python"
)

boost_headers(
    name = "pending"
)

boost_headers(
    name = "concept"
)

boost_headers(
    name = "lexical_cast"
)

boost_headers(
    name = "property_map"
)

boost_headers(
    name = "typeof"
)

boost_headers(
    name = "container"
)

boost_headers(
    name = "container_hash"
)

boost_headers(
    name = "parameter"
)

boost_headers(
    name = "unordered"
)

boost_headers(
    name = "multi_index"
)

boost_headers(
    name = "archive"
)

boost_headers(
    name = "serialization"
)

boost_headers(
    name = "algorithm"
)

boost_headers(
    name = "random"
)

boost_headers(
    name = "spirit"
)

boost_headers(
    name = "tti"
)

boost_headers(
    name = "xpressive"
)

boost_headers(
    name = "optional"
)

boost_library(
    name = "filesystem",
    deps = [
        ":system",
    ],
)

boost_library(
    name = "operators",
    deps = [
        ":operators-headers",
    ],
)

boost_library(
    name = "numeric",
    deps = [
        ":numeric-headers",
    ],
)

boost_library(
    name = "math",
    deps = [
        ":math-headers",
    ],
)

boost_library(
    name = "config",
    deps = [
        ":config-headers",
    ],
)

boost_library(
    name = "multiprecision",
    deps = [
        ":multiprecision-headers",
        ":headers-base",
        ":mpl-headers",
        ":type_traits-headers",
        ":utility-headers",
        ":range-headers",
        ":iterator-headers",
        ":concept-headers",
        ":lexical_cast-headers",
        ":numeric-headers",
        ":container-headers",
        ":math-headers",
        ":container_hash-headers",
        ":integer-headers",
        ":functional-headers",
    ],
)

boost_library(
    name = "preprocessor",
    deps = [
        ":preprocessor-headers",
    ],
)

boost_library(
    name = "program_options",
    deps = [
        ":headers-all",
    ],
)

boost_library(
    name = "regex",
    deps = [
        ":core-headers",
        ":container_hash-headers",
        ":detail-headers",
        ":exception-headers",
        ":functional-headers",
        ":headers-base",
        ":mpl-headers",
        ":predef-headers",
        ":preprocessor-headers",
        ":regex-headers",
        ":type_traits-headers",
        ":utility-headers",
    ],
)

boost_library(
    name = "system",
    deps = [
        ":headers-all",
    ],
)

boost_library(
    name = "context",
    exclude_srcs = [
        "libs/context/src/unsupported.cpp",
        "libs/context/src/untested.cpp",
        "libs/context/src/fiber.cpp",
        "libs/context/src/continuation.cpp",
    ],
    extra_srcs = glob([
        "libs/context/src/posix/*.cpp",
        "libs/context/src/asm/*x86_64_sysv_elf*",
    ]),
    deps = [
        ":context-headers",
        ":detail-headers",
        ":headers-base",
    ],
)

cc_library(
    name = "context_asm_x86_64_sysv_elf",
    srcs = glob([
        "libs/context/src/asm/*x86_64_sysv_elf*",
    ]),
    deps = [
    ],
)

boost_library(
    name = "thread",
    extra_hdrs = [
        "libs/thread/src/pthread/once_atomic.cpp",
    ],
    # Add source files for the pthread backend
    extra_srcs = glob([
        "libs/thread/src/pthread/once.cpp",
        "libs/thread/src/pthread/thread.cpp",
    ]),
    linkopts = [
        "-pthread",
    ],
    deps = [
        ":headers-all",
    ],
)

boost_library(
    name = "interprocess",
    deps = [
        ":interprocess-headers",
    ],
)

boost_library(
    name = "process",
    deps = [
        ":process-headers",
        ":filesystem",
    ],
)

boost_library(
    name = "property_tree",
    deps = [
        ":property_tree-headers",
    ],
)

boost_library(
    name = "date_time",
    deps = [
        ":date_time-headers",
    ],
)

boost_library(
    name = "python",
    deps = [
        ":python-headers",
        ":headers-base",
        ":mpl-headers",
        ":type_traits-headers",
        ":utility-headers",
        ":function-headers",
        ":preprocessor-headers",
        ":type_index-headers",
        ":bind-headers",
        ":iterator-headers",
        ":numeric-headers",
        ":tuple-headers",
        ":graph-headers",
        ":range-headers",
        ":pending-headers",
        ":concept-headers",
        ":lexical_cast-headers",
        ":functional-headers",
        ":property_map-headers",
        ":typeof-headers",
        ":container-headers",
        ":math-headers",
        ":parameter-headers",
        ":unordered-headers",
        ":multi_index-headers",
        ":archive-headers",
        ":serialization-headers",
        ":algorithm-headers",
        ":random-headers",
        ":spirit-headers",
        ":tti-headers",
        ":xpressive-headers",
        ":optional-headers",
    ],
    extra_srcs = glob([
        "libs/python/src/converter/*.cpp",
        "libs/python/src/object/*.cpp",
        # "libs/python/src/numpy/*.cpp",
    ])
)

boost_library(
    name = "iostreams",
    deps = [
        ":headers-all",
        "@com_taobao_aios//third_party/zstd",
        "@com_taobao_aios//third_party/lzma",
        "@bz2lib//:bzip2",
    ],
)