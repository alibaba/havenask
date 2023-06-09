load("@bazel_skylib//lib:paths.bzl", "paths")
load("@rules_pkg//:providers.bzl", "PackageArtifactInfo")
load("//bazel:defs.bzl", "copy_target_to")

BundleFilesInfo = provider(
    doc = """Provider representing the installation of one or more files to destination with attributes""",
    fields = {
        # This is a mapping of destinations to sources to allow for the same
        # target to be installed to multiple locations within a package within a
        # single provider.
        "dest_src_map": """Map of file destinations to sources.

        Sources are represented by bazel `File` structures.""",
    },
)

_DEFAULT_MTIME = -1

_PKGFILEGROUP_STRIP_ALL = "."

REMOVE_BASE_DIRECTORY = "\0"

HAS_XZ_SUPPORT = True
tar_filetype = (
    [".tar", ".tar.gz", ".tgz", ".tar.bz2", "tar.xz"] if HAS_XZ_SUPPORT else [".tar", ".tar.gz", ".tgz", ".tar.bz2"]
)

def _sp_files_only():
    return _PKGFILEGROUP_STRIP_ALL

def _sp_from_pkg(path = ""):
    if path.startswith("/"):
        return path[1:]
    else:
        return path

def _sp_from_root(path = ""):
    if path.startswith("/"):
        return path
    else:
        return "/" + path

strip_prefix = struct(
    _doc = """pkg_files `strip_prefix` helper.  Instructs `pkg_files` what to do with directory prefixes of files.

    Each member is a function that equates to:

    - `files_only()`: strip all directory components from all paths

    - `from_pkg(path)`: strip all directory components up to the current
      package, plus what's in `path`, if provided.

    - `from_root(path)`: strip beginning from the file's WORKSPACE root (even if
      it is in an external workspace) plus what's in `path`, if provided.

    Prefix stripping is applied to each `src` in a `pkg_files` rule
    independently.
 """,
    files_only = _sp_files_only,
    from_pkg = _sp_from_pkg,
    from_root = _sp_from_root,
)

def _owner(file):
    if file.owner == None:
        fail("File {} ({}) has no owner attribute; cannot continue".format(file, file.path))
    else:
        return file.owner

def _do_strip_prefix(path, to_strip, src_file):
    if to_strip == "" or to_strip == ".":
        # We were asked to strip nothing, which is valid.  Just return the
        # original path.
        return path

    path_norm = paths.normalize(path)
    to_strip_norm = paths.normalize(to_strip) + "/"

    if path_norm.startswith(to_strip_norm):
        return path_norm[len(to_strip_norm):]
    else:
        # Avoid user surprise by failing if prefix stripping doesn't work as
        # expected.
        #
        # We already leave enough breadcrumbs, so if File.owner() returns None,
        # this won't be a problem.
        fail("Could not strip prefix '{}' from file {} ({})".format(to_strip, str(src_file), str(src_file.owner)))

def _relative_workspace_root(label):
    # Helper function that returns the workspace root relative to the bazel File
    # "short_path", so we can exclude external workspace names in the common
    # path stripping logic.
    return paths.join("..", label.workspace_name) if label.workspace_name else ""

def _path_relative_to_package(file):
    # Helper function that returns a path to a file relative to its package.
    owner = _owner(file)
    return paths.relativize(
        file.short_path,
        paths.join(_relative_workspace_root(owner), owner.package),
    )

def _path_relative_to_repo_root(file):
    # Helper function that returns a path to a file relative to its workspace root.
    return paths.relativize(
        file.short_path,
        _relative_workspace_root(_owner(file)),
    )

def _quote(filename, protect = "="):
    """Quote the filename, by escaping = by \\= and \\ by \\\\"""
    return filename.replace("\\", "\\\\").replace(protect, "\\" + protect)

def _dest_path(f, strip_prefixes):
    prefixes = strip_prefixes + ["_virtual_includes/", "external/"]
    path = f.short_path
    for prefix in prefixes:
        idx = path.find(prefix)
        if idx != -1:
            path = path[idx + len(prefix):]
            # skip the repo name
            idx = path.find("/")
            if idx != -1:
                path = path[idx+1:]
    if path.startswith("../"):
        path = path[3:]
    return path

def _get_transitive_headers(deps):
    return depset(
        [],
        transitive = [dep[CcInfo].compilation_context.headers for dep in deps],
    )

def _get_transitive_py_sources(deps):
    return depset(
        [],
        transitive = [dep[PyInfo].transitive_sources for dep in deps],
    )

def mapping_files(srcs, deps, prefix, strip_prefix):
    dest_src_map = {}
    for s in deps:
        for dest,src in s[BundleFilesInfo].dest_src_map.items():
            dest_src_map[dest] = src

    if strip_prefix.startswith("/"):
        # Relative to workspace/repository root
        src_dest_paths_map = {src: paths.join(
            prefix,
            _do_strip_prefix(
                _path_relative_to_repo_root(src),
                strip_prefix[1:],
                src,
            ),
        ) for src in srcs}
    else:
        # Relative to package
        src_dest_paths_map = {src: paths.join(
            prefix,
            _do_strip_prefix(
                _path_relative_to_package(src),
                strip_prefix,
                src,
            ),
        ) for src in srcs}

    # At this point, we have a fully valid src -> dest mapping in src_dest_paths_map.
    #
    # Construct the inverse of this mapping to pass to the output providers, and
    # check for duplicated destinations.
    for src, dest in src_dest_paths_map.items():
        # print(src, ' -> ', dest)
        if dest in dest_src_map:
            fail(("Multiple sources (at least {0}, {1}) map to the same destination {2}.  " +
                 "Consider adjusting strip_prefix and/or renames").format(
                        dest_src_map[dest].path, src.path, dest))
        dest_src_map[dest] = src
    return dest_src_map;

def _get_srcs_from_group(ctx):
    src_files = [f for f in ctx.files.srcs if f not in ctx.files.excludes]
    src_deps = []
    for src in ctx.attr.srcs:
        if (OutputGroupInfo in src):
            set_list = []
            for group_name in src[OutputGroupInfo]:
                src_deps.append(src[OutputGroupInfo][group_name])
    src_set = depset(src_files, transitive = src_deps)
    return src_set.to_list()

def _bundle_files_impl(ctx):
    # Exclude excludes
    srcs = []
    if (ctx.attr.add_src_from_group):
        srcs = _get_srcs_from_group(ctx)
    else:
        srcs = [f for f in ctx.files.srcs if f not in ctx.files.excludes]
    dest_src_map = mapping_files(srcs, ctx.attr.deps, ctx.attr.prefix, ctx.attr.strip_prefix)
    return [
        BundleFilesInfo(
            dest_src_map = dest_src_map,
        ),
    ]

def _bundle_transitive_files(ctx, transitive_func):
    # Exclude excludes
    srcs = [f for f in ctx.attr.srcs if f not in ctx.attr.excludes]
    srcs = [f for f in transitive_func(srcs).to_list()
            if f not in ctx.files.excludes]
    src_dest_paths_map = {src: paths.join(
        ctx.attr.prefix,
        _dest_path(src, ctx.attr.strip_prefixes),
    ) for src in srcs}
    dest_src_map = {}
    for s in ctx.attr.deps:
        for dest,src in s[BundleFilesInfo].dest_src_map.items():
            dest_src_map[dest] = src
    for src, dest in src_dest_paths_map.items():
        dest_src_map[dest] = src
    return [
        BundleFilesInfo(
            dest_src_map = dest_src_map,
        ),
    ]

def _bundle_cc_hdrs_impl(ctx):
    return _bundle_transitive_files(ctx, _get_transitive_headers)

def _bundle_py_libs_impl(ctx):
    return _bundle_transitive_files(ctx, _get_transitive_py_sources)

_bundle_rules_base_attrs =  {
    "srcs": attr.label_list(
        doc = """Files/Labels to include in the outputs of these rules""",
        default = [],
        allow_files = True,
    ),
    "deps": attr.label_list(
        doc = "",
        default = [],
        providers = [BundleFilesInfo, DefaultInfo],
    ),
    "prefix": attr.string(),
    "excludes": attr.label_list(
        doc = """List of files or labels to exclude from the inputs to this rule.

            Mostly useful for removing files from generated outputs or
            preexisting `filegroup`s.
            """,
        default = [],
        allow_files = True,
    ),
    "execution_requirements": attr.string_dict(),
}

_bundle_files_attrs = {
    "strip_prefix": attr.string(
        doc = """What prefix of a file's path to discard prior to installation.
            """,
        default = strip_prefix.files_only(),
    ),
    "add_src_from_group": attr.bool(
        default = False
    )
}
_bundle_files_attrs.update(_bundle_rules_base_attrs)

_bundle_transitive_attrs = {
    "strip_prefixes": attr.string_list(),
}
_bundle_transitive_attrs.update(_bundle_rules_base_attrs)

bundle_files_impl = rule(
    implementation = _bundle_files_impl,
    attrs = _bundle_files_attrs,
    provides = [BundleFilesInfo],
)

bundle_cc_hdrs_impl = rule(
    implementation = _bundle_cc_hdrs_impl,
    attrs = _bundle_transitive_attrs,
    provides = [BundleFilesInfo],
)

bundle_py_libs_impl = rule(
    implementation = _bundle_py_libs_impl,
    attrs = _bundle_transitive_attrs,
    provides = [BundleFilesInfo],
)

def _bundle_install_impl(ctx):
    output_file = ctx.outputs.out
    src_files = []
    dest_files = []
    output_content = []
    dest_src_map = {}
    for dest_srcs in [e[BundleFilesInfo].dest_src_map for e in ctx.attr.srcs]:
        for dest, src in dest_srcs.items():
            if dest.startswith('/'):
                dest = dest[1:]
            if dest in dest_src_map:
                old_src = dest_src_map[dest]
                if src != old_src:
                    # print("files '%s' is generated by conflicting sources:\n%s\n%s" % (
                    #      dest, old_src, src))
                    pass
                continue
            else:
                dest_src_map[dest] = src
            dest_file = ctx.actions.declare_file(dest)
            output_content.append("%s -> %s" % (src.path, dest_file.path))
            src_files.append(src)
            dest_files.append(dest_file)
    arg_file = ctx.actions.declare_file(ctx.label.name + ".args")
    ctx.actions.write(arg_file, "\n".join(output_content))
    ctx.actions.run(
        mnemonic = "BundleInstall",
        progress_message = "Installing: %s" % ctx.label.name,
        inputs = src_files + [arg_file],
        outputs = dest_files,
        executable = ctx.executable.install_bin,
        arguments = [arg_file.path],
        use_default_shell_env = True,
    )
    ctx.actions.run_shell(
        mnemonic = "WriteFile",
        inputs = dest_files,
        outputs = [output_file],
        command = "touch %s" % output_file.path,
        arguments = [],
        execution_requirements = ctx.attr.execution_requirements,
    )
    return [
        DefaultInfo(
            files = depset(dest_files + [arg_file]),
            runfiles = ctx.runfiles(files = dest_files + [arg_file]),
        ),
    ]

def _bundle_tar_impl(ctx):
    output_file = ctx.outputs.out
    # Start building the arguments.
    args = [
        "--root_directory=" + ctx.attr.package_base,
        "--output=" + output_file.path,
        "--mode=" + ctx.attr.mode,
        "--owner=" + ctx.attr.owner,
        "--owner_name=" + ctx.attr.ownername,
    ]
    if ctx.attr.mtime != _DEFAULT_MTIME:
        if ctx.attr.portable_mtime:
            fail("You may not set both mtime and portable_mtime")
        args.append("--mtime=%d" % ctx.attr.mtime)
    if ctx.attr.portable_mtime:
        args.append("--mtime=portable")

    files = []
    files_map = {}
    for dest_srcs in [e[BundleFilesInfo].dest_src_map for e in ctx.attr.srcs]:
        files_map.update(dest_srcs)
    files += files_map.values()

    entries = [
        [0, dest, src.path, '', '', '']
        for dest, src in files_map.items()
    ]
    if ctx.attr.modes:
        args += [
            "--modes=%s=%s" % (_quote(key), ctx.attr.modes[key])
            for key in ctx.attr.modes
        ]
    if ctx.attr.owners:
        args += [
            "--owners=%s=%s" % (_quote(key), ctx.attr.owners[key])
            for key in ctx.attr.owners
        ]
    if ctx.attr.ownernames:
        args += [
            "--owner_names=%s=%s" % (_quote(key), ctx.attr.ownernames[key])
            for key in ctx.attr.ownernames
        ]
    if ctx.attr.extension:
        _, _, extension = ctx.attr.extension.rpartition('.')
        if extension in ["tgz", "gz"]:
            args += ["--compressor=pigz"]
        else:
            args += ["--compression=%s" % extension]
    args += ["--tar=" + f.path for f in ctx.files.deps]
    # symlinks = { dest : src }
    entries += [
        [1, k, v, '', '', '']
        for k, v in ctx.attr.symlinks.items()
    ]

    arg_file = ctx.actions.declare_file(ctx.label.name + ".args")
    ctx.actions.write(arg_file, "\n".join(args))
    manifest_file = ctx.actions.declare_file(ctx.label.name + ".manifest")
    ctx.actions.write(manifest_file, json.encode(entries))
    ctx.actions.run(
        mnemonic = "PackageTar",
        progress_message = "Writing: %s" % output_file.path,
        inputs = files + [arg_file, manifest_file],
        executable = ctx.executable.build_tar,
        arguments = [
            "@" + arg_file.path,
            '--manifest', manifest_file.path
        ],
        outputs = [output_file],
        env = {
            "LANG": "en_US.UTF-8",
            "LC_CTYPE": "UTF-8",
            "PYTHONIOENCODING": "UTF-8",
            "PYTHONUTF8": "1",
        },
        use_default_shell_env = True,
        execution_requirements = ctx.attr.execution_requirements,
    )
    return [
        DefaultInfo(
            files = depset([output_file]),
            runfiles = None,
        ),
        PackageArtifactInfo(
            label = ctx.label.name,
            file_name = output_file.basename,
        ),
    ]

def _bundle_unpack_impl(ctx):
    src_files = []
    dest_files = []
    for dest_srcs in [e[BundleFilesInfo].dest_src_map for e in ctx.attr.srcs]:
        for dest, src in dest_srcs.items():
            src_files.append(src)
            dest = paths.join(ctx.attr.prefix, dest)
            dest_files.append(ctx.actions.declare_file(dest))

    dst2src = {}
    for dst, src in zip(dest_files, src_files):
        if dst.path not in dst2src:
            ctx.actions.symlink(
                output=dst,
                target_file=src,
            )
            dst2src[dst.path] = src

    return [
        DefaultInfo(
            files = depset(dest_files),
            runfiles = ctx.runfiles(files = dest_files),
        )
    ]

bundle_install_impl = rule(
    attrs = {
        "srcs": attr.label_list(
            doc = """Mapping groups to include in this RPM.

            These are typically brought into life as `bundle_filegroup`s.
            """,
            mandatory = True,
            providers = [DefaultInfo],
        ),
        "out": attr.output(mandatory = True),
        # Implicit dependencies.
        "install_bin": attr.label(
            default = Label("//bazel:install"),
            cfg = "exec",
            executable = True,
            allow_files = True,
        ),
        "execution_requirements": attr.string_dict(),
    },
    executable = False,
    implementation = _bundle_install_impl,
)

bundle_tar_impl = rule(
    implementation = _bundle_tar_impl,
    attrs = {
        "package_base": attr.string(default = "./"),
        "srcs": attr.label_list(allow_files = True),
        "deps": attr.label_list(allow_files = tar_filetype),
        "mode": attr.string(default = "0555"),
        "modes": attr.string_dict(),
        "mtime": attr.int(default = _DEFAULT_MTIME),
        "portable_mtime": attr.bool(default = True),
        "owner": attr.string(default = "0.0"),
        "ownername": attr.string(default = "."),
        "owners": attr.string_dict(),
        "ownernames": attr.string_dict(),
        "extension": attr.string(default = "tar"),
        "symlinks": attr.string_dict(),
        "empty_files": attr.string_list(),
        # Common attributes
        "out": attr.output(mandatory = True),

        # Implicit dependencies.
        "build_tar": attr.label(
            default = Label("@rules_pkg//pkg/private:build_tar"),
            cfg = "exec",
            executable = True,
            allow_files = True,
        ),

        "execution_requirements": attr.string_dict(),
    },
    provides = [PackageArtifactInfo],
)

bundle_unpack_impl = rule(
    implementation = _bundle_unpack_impl,
    attrs = {
        "srcs": attr.label_list(),
        "prefix": attr.string(default = "./"),

        "execution_requirements": attr.string_dict(),
    },
)

# Add tags by default to reduce the burden of compilation in development stage
def _add_default_args(kwargs):
    key = "execution_requirements"
    values = kwargs.get(key) or {}
    kwargs.setdefault(key, values)

    key = "tags"
    tags = kwargs.get(key) or []
    tags.extend(["manual"])
    kwargs.setdefault(key, tags)

def bundle_files(name, **kwargs):
    _add_default_args(kwargs)
    bundle_files_impl(
        name = name,
        **kwargs,
    )

def bundle_cc_hdrs(name, **kwargs):
    _add_default_args(kwargs)
    bundle_cc_hdrs_impl(
        name = name,
        **kwargs,
    )

def bundle_py_libs(name, **kwargs):
    _add_default_args(kwargs)
    bundle_py_libs_impl(
        name = name,
        **kwargs,
    )

def bundle_unpack(name, **kwargs):
    _add_default_args(kwargs)
    bundle_unpack_impl(
        name = name,
        **kwargs,
    )

"""
    install file with bundle_files declaration under package dir,
    will clear files under package dir except those declared in bundle_install.
    DO NOT declare more than one bundle_install target in same BUILD file.
    DO NOT declare target not in bundle_install in same BUILD file.
"""
def bundle_install(name, **kwargs):
    _add_default_args(kwargs)
    # local : need to clean cache directory out of sandbox
    kwargs["tags"] = kwargs.get("tags") + ["local", "no-remote-cache"]
    bundle_install_impl(
        name = name,
        out = name + ".out",
        **kwargs,
    )

def bundle_tar(name, **kwargs):
    extension = kwargs.get("extension") or "tar"
    _add_default_args(kwargs)

    bundle_tar_impl(
        name = name,
        out = kwargs.pop("out", None) or (name + "." + extension),
        **kwargs,
    )
