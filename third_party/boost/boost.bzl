DEFAULT_COPTS = [
    "-Wno-tautological-constant-out-of-range-compare",
    "-Wno-return-std-move",
    "-Wno-unused-value",
    "-Wno-parentheses",
    "-Wno-deprecated-declarations",
    "-I/usr/include/python3.6m",
    # "-D_GLIBCXX_USE_CXX11_ABI=0",
]

def boost_library(name, exclude_srcs=None, defines=None, deps=None, extra_srcs=None, extra_hdrs=None, linkopts=None):
    if exclude_srcs == None:
        exclude_srcs = []

    if defines == None:
        defines = []

    if deps == None:
        deps = []

    if extra_srcs == None:
        extra_srcs = []

    if extra_hdrs == None:
        extra_hdrs = []

    if linkopts == None:
        linkopts = []

    return native.cc_library(
        name = name,
        visibility = ["//visibility:public"],
        defines = defines,
        hdrs = native.glob([
            x % name
            for x in [
                'libs/%s/src/*.hpp',
                'boost/%s/**/*.ipp',
            ]
        ]) + extra_hdrs,
        srcs = native.glob([
            x % name
            for x in [
                'libs/%s/src/*.cpp',
                'boost/%s/detail/*.hpp',
            ]
        ], exclude_srcs) + extra_srcs,
        deps = deps,
        copts = DEFAULT_COPTS,
        linkopts = linkopts,
        alwayslink = True
    )

def boost_headers(name, deps = None):
    if deps == None:
        deps = []

    return native.cc_library(
        name = name + "-headers",
        hdrs = native.glob([
            "boost/%s/*.h" % name,
            "boost/%s/*.hpp" % name,
            "boost/%s/*.ipp" % name,
            "boost/%s/**/*.h" % name,
            "boost/%s/**/*.hpp" % name,
            "boost/%s/**/*.ipp" % name,
        ]),
        deps = deps,
        include_prefix = "boost",
        # includes: list of include dirs added to the compile line
        includes = [
            ".",
        ],
        strip_include_prefix = "boost",
    )
