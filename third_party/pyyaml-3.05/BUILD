'''
更高版本的yaml会自动安装python3版本，无法使用
仅支持python2的yaml 3.05已经无法通过pip下载，因此通过http_archive临时解决
使用的时候需要增加usr/local/lib/python/site-packages/lib到扫描路径列表第一位
'''

py_library(
    name = "pyyaml",
    srcs = glob([
        "lib/yaml/*.py"
    ]),
    visibility = ["//visibility:public"]
)