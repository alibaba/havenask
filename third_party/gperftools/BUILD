load("@rules_pkg//:pkg.bzl", "pkg_tar")
package(default_visibility = ["//visibility:public"])

hdrs = [
     "gperftools/heap-checker.h",
     "gperftools/heap-profiler.h",
     "gperftools/malloc_extension_c.h",
     "gperftools/malloc_extension.h",
     "gperftools/malloc_hook_c.h",
     "gperftools/malloc_hook.h",
     "gperftools/nallocx.h",
     "gperftools/profiler.h",
     "gperftools/stacktrace.h",
     "gperftools/tcmalloc.h",
     "google/heap-checker.h",
     "google/heap-profiler.h",
     "google/malloc_extension_c.h",
     "google/malloc_extension.h",
     "google/malloc_hook_c.h",
     "google/malloc_hook.h",
     "google/profiler.h",
     "google/stacktrace.h",
     "google/tcmalloc.h",
]

cc_import(
    name = "tcmalloc_so",
    shared_library = "usr/local/lib64/libtcmalloc.so.4",
)

cc_library(
    name = "gperftools",
    hdrs = glob([
        "include/google/*.h",
        "include/gperftools/*.h",
    ]),
    deps = [":tcmalloc_so"],
    strip_include_prefix = "include",
)

filegroup(
    name="gperftools_lib",
    srcs=glob([
        "usr/local/lib64/libtcmalloc.so",
        "usr/local/lib64/libtcmalloc.so.4",
        "usr/local/lib64/libtcmalloc_minimal.so",
        "usr/local/lib64/libtcmalloc_debug.so",
        "usr/local/lib64/libtcmalloc_and_profiler.so",
    ]),
    visibility = ["//visibility:public"],
)
