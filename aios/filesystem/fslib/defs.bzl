def fslib_enable_async(a, b=[]):
    return select({
        "//:fslib_enable_async": a,
        "//:fslib_disable_async": b,
        "//conditions:default": b,
    })
