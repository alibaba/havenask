load("@rules_cc//cc:defs.bzl", "cc_library")
load("//bazel:defs.bzl", "cc_fast_test", "cc_slow_test")

# -Wsign-compare, -Wstringop-overflow, -Wstringop-truncation
common_copts = ["-Werror", "-Wunused-variable", "-Wsuggest-override", "-Wregister"]

# By default, srcs use [<name>.cpp], hdrs use [<name>.h], copts use common_copts,
def indexlib_cc_library(name, srcs = None, hdrs = None, copts = [], **kwargs):
    cc_library(
        name = name,
        srcs = [name + ".cpp"] if srcs == None else srcs,
        hdrs = [name + ".h"] if hdrs == None else hdrs,
        copts = common_copts + copts,
        **kwargs
    )

def indexlib_cc_fast_test(copts = [], **kwargs):
    cc_fast_test(
        copts = common_copts + copts,
        **kwargs
    )

def indexlib_cc_slow_test(copts = [], **kwargs):
    cc_slow_test(
        copts = common_copts + copts,
        **kwargs
    )
