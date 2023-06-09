
def _py_pacakge_impl(ctx):
    in_file_list = []
    for dep_file in ctx.files.deps:
        in_file_list.append(dep_file.path)
    in_files = " ".join(in_file_list)
    out_file = ctx.actions.declare_file("%s.tar" % ctx.label.name)

    transform_arg = ""
    if ctx.attr.package_dir != None:
        pacakged_dir = ctx.attr.package_dir
        transform_arg = "--transform='s/bazel-out\/k8-fastbuild\/bin\(\/%s\)\?/%s/g'" \
                        % (pacakged_dir, pacakged_dir)
    ctx.actions.run_shell(
        inputs = ctx.files.deps,
        outputs = [out_file],
        command = "bash -c \"set -xe && "+ \
                  "tar --dereference %s  -cvf %s %s\"" \
                  % (transform_arg, out_file.path, in_files),
    )
    return [DefaultInfo(files = depset([out_file]))]

py_tarball = rule(
    implementation = _py_pacakge_impl,
    attrs = {
        "deps": attr.label_list(),
        "package_dir": attr.string(),
    }
)

