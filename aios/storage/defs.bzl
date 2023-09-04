load("@rules_cc//cc:defs.bzl", "cc_library")
load("//bazel:defs.bzl", "cc_fast_test", "cc_slow_test")

# mv to strict_cc_library, onefile_cc_library
# -Wsign-compare, -Wstringop-overflow, -Wstringop-truncation
common_copts = ["-Werror", "-Wunused-variable", "-Wsuggest-override", "-Wregister"]

# By default, srcs use [<name>.cpp], hdrs use [<name>.h], copts use common_copts,
def strict_cc_library(name, srcs = None, hdrs = None, copts = [], export_name = None, export_visibility = None, **kwargs):
    cc_library(
        name = name,
        srcs = [name + ".cpp"] if srcs == None else srcs,
        hdrs = [name + ".h"] if hdrs == None else hdrs,
        copts = common_copts + copts,
        **kwargs
    )
    if export_name != None:
        native.alias(
            name = export_name,
            actual = ":" + name,
            visibility = export_visibility,
        )

def strict_cc_fast_test(copts = [], **kwargs):
    cc_fast_test(
        copts = common_copts + copts,
        **kwargs
    )

def strict_cc_slow_test(copts = [], **kwargs):
    cc_slow_test(
        copts = common_copts + copts,
        **kwargs
    )
