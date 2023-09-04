load("//bazel:bundle.bzl", "bundle_tar")

def bundle_version_tar(name, **kwargs):
    extension = kwargs.get("extension")
    bundle_tar(
        name = name,
        out = kwargs.pop("out", None) or (name + "-" + extension + ".tar"),
        **kwargs,
    )
