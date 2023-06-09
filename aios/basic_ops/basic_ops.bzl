def register_custom_op(op_dir_path,
                       srcs = None,
                       hdrs = None,
                       deps = [],
                       **kwargs):
    op_name = op_dir_path.split('/')[-1]
    if not op_name.endswith("_op"):
        op_name = op_name + "_op"

    native.cc_library(
        name = op_name + "_opdef",
        srcs = native.glob([op_dir_path + "/OpDefs.cpp"]),
        visibility = ["//visibility:public"],
        deps = [
            "@com_google_protobuf//:protobuf_headers",
            "@flatbuffers",
            "@org_tensorflow//tensorflow/core:framework_headers_lib",
            "@org_tensorflow//third_party/eigen3",
        ],
        alwayslink = True,
    )

    if srcs == None:
        srcs = native.glob([
            op_dir_path + "/*.cpp"
        ], exclude = [
            op_dir_path + "/OpDefs.cpp"
        ])

    if hdrs == None:
        hdrs = native.glob([
            op_dir_path + "/*.h"
        ])
        
    native.cc_library(
        name = op_name + "_kernel",
        srcs = srcs,
        hdrs = hdrs,
        deps = deps,
        alwayslink = True,
        **kwargs
    )

    native.cc_library(
        name = op_name,
        deps = [
            ":" + op_name + "_kernel",
            ":" + op_name + "_opdef",
        ],
        visibility = ["//visibility:public"],
        alwayslink = True,
        **kwargs
    )
