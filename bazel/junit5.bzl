def java_junit5_test(name, srcs, test_package, deps = [], runtime_deps = [],
                     deps_name = "maven",
                     **kwargs):
    FILTER_KWARGS = [
        "main_class",
        "use_testrunner",
        "args",
    ]

    for arg in FILTER_KWARGS:
        if arg in kwargs.keys():
            kwargs.pop(arg)

    junit_console_args = []
    if test_package:
        junit_console_args += ["--select-package", test_package, "-n", ".*"]
    else:
        fail("must specify 'test_package'")

    native.java_test(
        name = name,
        srcs = srcs,
        use_testrunner = False,
        main_class = "org.junit.platform.console.ConsoleLauncher",
        args = junit_console_args,
        deps = deps + [
             "@" + deps_name + "//:org_junit_jupiter_junit_jupiter_api",
             "@" + deps_name + "//:org_junit_jupiter_junit_jupiter_params",
             "@" + deps_name + "//:org_junit_jupiter_junit_jupiter_engine",
        ],
        runtime_deps = runtime_deps + [
             "@" + deps_name + "//:org_junit_platform_junit_platform_console"
        ],
        **kwargs
    )
