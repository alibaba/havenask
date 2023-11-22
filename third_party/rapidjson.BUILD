cc_library(
    name = "rapidjson",
    hdrs = glob(["include/rapidjson/**/*.h"]),
    defines = [
        "RAPIDJSON_HAS_STDSTRING=1",
        # kWriteNanAndInfFlag, ref `https://github.com/Tencent/rapidjson/issues/905`
        "RAPIDJSON_WRITE_DEFAULT_FLAGS=2",
    ],
    includes = ["include"],
    visibility = ["//visibility:public"],
)
