# global platform related opts configured here
#   others may configure in .bazelrc

#load("@org_tensorflow//tensorflow/core:platform/default/build_config.bzl", "tf_proto_library")
load("//bazel:tf_proto.bzl", "tf_proto_library")

load("@rules_cc//examples:experimental_cc_shared_library.bzl", "cc_shared_library")
load("@pip//:requirements.bzl", "requirement")

def if_clang(a, b=[]):
    return select({
        "//:clang_build": a,
        "//conditions:default": b,
    })

def if_arm(a, b=[]):
    return select({
        "//:arm_cpu": a,
        "//conditions:default": b,
    })

def new_abi_flag():
    return select({
        "//:new_abi_mode": 1,
        "//conditions:default": 0,
    })

def if_rdma(a, b=[]):
    return select({
        "//:enable_rdma" : a,
        "//conditions:default" : b,
    })

def if_solar(a, b=[]):
    return select({
        "//:enable_solar" : a,
        "//conditions:default" : b,
    })

def if_remote_exec(a, b):
    return select({
        "//:remote_exec" : a,
        "//conditions:default" : b,
    })

def aios_opts():
    return []

def enable_thread_safety():
    return select({
        "//:clang_build": [
            "-Wthread-safety-precise",
            "-Wthread-safety-analysis",
        ],
        "//conditions:default": [],
    })

def if_enable_aicompiler(if_true, if_false = []):
    return select({
        "//:build_with_aicompiler": if_true,
        "//conditions:default": if_false,
    })

def if_enable_ppu(if_true, if_false = []):
    return select({
        "//:build_with_ppu": if_true,
        "//conditions:default": if_false,
    })

def gen_error_info(name, srcs, outs):
    cmd = "python2 $(location //bazel:gen_error_info_py) $(location %s) $(@D)/%s" % (srcs[0], outs[0])
    native.genrule(
        name = name,
        outs = outs,
        srcs = srcs + ["//bazel:gen_error_info_py"],
        cmd = cmd
    )

def cc_proto(
        name,
        srcs,
        deps=None,
        include = ".",
        import_prefix=None,
        strip_import_prefix=None,
        visibility=None,
        use_grpc_plugin=False,
        cc_compile_grpc=True,
        py_compile_grpc=True):
    cc_compile_grpc = cc_compile_grpc if use_grpc_plugin else False
    py_compile_grpc = py_compile_grpc if use_grpc_plugin else False
    tf_proto_library(
        name = name,
        srcs = srcs,
        protodeps = deps,
        visibility = visibility,
        include = include,
        cc_grpc_version = cc_compile_grpc,
        has_services = py_compile_grpc,
    )
    native.cc_library(
        name = name + "_cc_proto",
        hdrs = [s[:-len(".proto")] + ".pb.h" for s in srcs],
        deps = [name + "_cc_impl"],
        include_prefix = import_prefix,
        strip_include_prefix = strip_import_prefix,
        visibility = visibility,
    )
    header_deps = []
    for dep in deps:
        if str(dep).startswith("//"):
            header_deps.append(dep + "_cc_proto_headers")
        else:
            # tensorflow proto library
            header_deps.append(dep + "_cc_headers_only")
    native.cc_library(
        name = name + "_cc_proto_headers",
        hdrs = [s[:-len(".proto")] + ".pb.h" for s in srcs],
        deps = header_deps,
        include_prefix = import_prefix,
        strip_include_prefix = strip_import_prefix,
        visibility = visibility,
    )
    return name + "_cc_proto"

def cc_and_py_proto(
        name,
        srcs,
        deps=None,
        include = ".",
        import_prefix=None,
        strip_import_prefix=None,
        visibility=None,
        use_grpc_plugin=False,
        cc_compile_grpc=True,
        py_compile_grpc=True):
    cc_compile_grpc = cc_compile_grpc if use_grpc_plugin else False
    py_compile_grpc = py_compile_grpc if use_grpc_plugin else False
    tf_proto_library(
        name = name,
        srcs = srcs,
        protodeps = deps,
        visibility = visibility,
        include = include,
        cc_grpc_version = cc_compile_grpc,
        has_services = py_compile_grpc,
    )
    native.cc_library(
        name = name + "_cc_proto",
        hdrs = [s[:-len(".proto")] + ".pb.h" for s in srcs],
        deps = [name + "_cc_impl"],
        include_prefix = import_prefix,
        strip_include_prefix = strip_import_prefix,
        visibility = visibility,
    )
    header_deps = []
    for dep in deps:
        if str(dep).startswith("//"):
            header_deps.append(dep + "_cc_proto_headers")
        else:
            # tensorflow proto library
            header_deps.append(dep + "_cc_headers_only")
    native.cc_library(
        name = name + "_cc_proto_headers",
        hdrs = [s[:-len(".proto")] + ".pb.h" for s in srcs],
        deps = header_deps,
        include_prefix = import_prefix,
        strip_include_prefix = strip_import_prefix,
        visibility = visibility,
    )
    native.py_library(
        name=name + "_py_proto",
        srcs=[s[:-len(".proto")] + "_pb2.py" for s in srcs],
        deps=[dep + "_py_proto" for dep in deps] if deps else None,
        imports=(["."] if import_prefix == None else [import_prefix]) + ([".."] if strip_import_prefix == None else [strip_import_prefix]),
        visibility=visibility,
    )

def rpm_library(
        name,
        hdrs,
        include_path=None,
        lib_path=None,
        rpms=None,
        static_lib=None,
        static_libs=[], # multi static libs, do not add to cc_library, provide .a filegroup
        shared_lib=None,
        shared_libs=[],
        bins=[],
        include_prefix=None,
        static_link=False,
        deps=[],
        header_only=False,
        tags={},
        **kwargs):
    hdrs = [ "include/" + hdr for hdr in hdrs ]
    outs = [] + hdrs
    if static_lib:
        outs.append(static_lib)
    if shared_lib :
        outs.append(shared_lib)
    if not rpms:
        rpms = ["@" + name + "//file:file"]
    bash_cmd = "mkdir " + name + " && cd " + name
    bash_cmd += " && for e in $(SRCS); do rpm2cpio ../$$e | cpio -idm; done"
    if include_path != None:
        if header_only:
            bash_cmd += "&& cp -rf " + include_path + "/* ../$(@D)/"
        else:
            bash_cmd += "&& cp -rf " + include_path + "/* ../$(@D)/include"
    if len(static_libs) > 0:
        # extract all .a files to its own directory in case .o file conflict, and ar them together to target .a file.
        bash_cmd += "&& for a in " + " ".join(static_libs) + "; do d=$${a%.a} && mkdir $$d && cd $$d && ar x ../" + lib_path + "$$a && cd -; done && ar rc ../$(@D)/" + static_lib + " */*.o"
    elif static_lib:
        bash_cmd += "&& cp -L " + lib_path + "/*.a" + " ../$(@D)/"
    if shared_lib:
        bash_cmd += "&& cp -L " + lib_path + "/" + shared_lib + " ../$(@D) && patchelf --set-soname " + shared_lib + " ../$(@D)/" + shared_lib
    for share_lib in shared_libs:
        outs.append(share_lib)
        bash_cmd += "&& cp -L " + lib_path + "/" + share_lib + " ../$(@D) && patchelf --set-soname " + share_lib + " ../$(@D)/" + share_lib
    for path in bins:
        outs.append(path)
        bash_cmd += "&& cp -rL " + path + " ../$(@D)"
    bash_cmd += " && cd -"

    native.genrule(
        name = name + "_files",
        srcs = rpms,
        outs = outs,
        cmd = bash_cmd,
        visibility = ["//visibility:public"],
        tags=tags,
    )
    hdrs_fg_target = name + "_hdrs_fg"
    native.filegroup(
        name = hdrs_fg_target,
        srcs = hdrs,
    )
    if static_lib:
        native.filegroup(
            name = name + "_static",
            srcs = [static_lib],
            visibility = ["//visibility:public"],
        )
    srcs = []
    shared_files = shared_libs + (shared_lib and [shared_lib] or [])
    if shared_files:
        shared_filegroup = name + "_shared"
        native.filegroup(
            name = shared_filegroup,
            srcs = shared_files,
            visibility = ["//visibility:public"],
        )
        if shared_libs:
            srcs.append(shared_filegroup)

    if bins:
        bins_filegroup = name + "_bins"
        native.filegroup(
            name = bins_filegroup,
            srcs = bins,
            visibility = ["//visibility:public"],
            tags=tags,
        )

    if static_lib == None:
        native.cc_library(
            name = name,
            hdrs = [hdrs_fg_target],
            srcs = shared_files,
            deps = deps,
            strip_include_prefix = "include",
            include_prefix = include_prefix,
            visibility = ["//visibility:public"],
            **kwargs
        )
    else:
        import_target = name + "_import"
        alwayslink = static_lib!=None
        native.cc_import(
            name = import_target,
            static_library = static_lib,
            shared_library = shared_lib,
            alwayslink=alwayslink,
            visibility = ["//visibility:public"],
        )
        native.cc_library(
            name = name,
            hdrs = [hdrs_fg_target],
            srcs = srcs,
            deps = deps + [import_target],
            visibility = ["//visibility:public"],
            strip_include_prefix = "include",
            include_prefix = include_prefix,
            **kwargs
        )


# do substitution
def gen_conf(name, srcs, outs, args = None):
    conf_target = name
    if args :
        sed_args = "; ".join(["s|{}|{}|g".format(k, v) for k, v in args.items()])
        cmd = "sed -e '%s' $(SRCS) > \"$@\"" % sed_args
    else :
        cmd = "cp $(SRCS) \"$@\""
    native.genrule(
        name = conf_target,
        srcs = srcs,
        outs = outs,
        cmd = cmd,
    )
    return conf_target

# generate header lib by conf. eg: test.h.in
def gen_conf_lib(name, srcs, outs,
    data = None,
    args = None,
    include_prefix = None,
    strip_include_prefix = None):
    conf_target = gen_conf(name + "_conf", srcs, outs, args)
    lib_target = name
    native.cc_library(
        name = lib_target,
        srcs = [conf_target],
        hdrs = [conf_target],
        data = data,
        strip_include_prefix = strip_include_prefix,
        include_prefix = include_prefix,
        visibility = ["//visibility:public"],
        linkstatic = True,
    )
    return lib_target

# do substitution for a batch of files. eg: cm2_client_config.xml.in
def batch_gen_conf(name, srcs, suffix = ".in", args = None):
    targets = []
    i = 0
    for src in srcs:
        if not src.endswith(suffix):
            continue
        out = src.rstrip(suffix);
        conf_target = gen_conf(name + "_sub_" + str(i), [src,], [out,], args)
        targets.append(conf_target)
        i += 1
    native.filegroup(
        name = name,
        srcs = targets,
    )

# The 0th src of genlex must be the lexer input.
def genlex(name, src, lexflags=" -8 -Cfa"):
    vec = src.split("/")
    file_name = vec[-1]
    path = src[0:len(src) - len(file_name)]
    new_file_name = file_name[0:-3] + ".cc"
    cmd = "flex %s -o $(@D)/%s $(location %s) " % (lexflags, new_file_name, src)
    native.genrule(
        name = name,
        outs = [new_file_name],
        srcs = [src],
        cmd = cmd,
        visibility = ["//visibility:public"],
    )

def genyacc(name, src, hh_files = ["location.hh", "position.hh", "stack.hh"], path=""):
    vec = src.split("/")
    file_name = vec[-1]
    path = path if len(path) != 0 else src[0:len(src) - len(file_name)]
    if path and not path.endswith("/"):
        path += "/"
    class_name = file_name[0:-3]
    new_file_name = path + class_name + ".cc"
    # bison 3.2+ supported
    opt = "" if "location.hh" in hh_files else "-Dapi.location.file=none"
    cmd = ""
    if path:
        cmd += "mkdir -p %s &&" % path
    cmd += "bison -o $(@D)/%s $(location %s) %s && sed 's/include \"%s/include \"%s%s/' $(@D)/%s -i" % (new_file_name, src, opt, class_name, path.replace("/", "\\/"), class_name, new_file_name)
    hdrs = [class_name + ".hh"] + hh_files
    hdrs = [path + hh for hh in hdrs]
    native.genrule(
        name = name,
        outs = [new_file_name] + hdrs,
        srcs = [src],
        cmd = cmd,
        visibility = ["//visibility:public"],
    )
    native.filegroup(
        name = name + "_srcs",
        srcs = [new_file_name]
    )
    native.filegroup(
        name = name + "_hdrs_tmp",
        srcs = hdrs
    )
    native.cc_library(
        name = name + "_hdrs",
        hdrs = [name + "_hdrs_tmp"],
        visibility = ["//visibility:public"],
        strip_include_prefix = path,
        include_prefix = path,
    )

def cc_fast_test(**args):
    args["timeout"] = "long"
    native.cc_test(**args)

def cc_slow_test(**args):
    if args.get("tags"):
       args["tags"].append("slow_case")
    else:
        args["tags"] = ["slow_case"]
    args["timeout"] = "long"
    native.cc_test(**args)

# Used for cases error in sandbox such as directio cases.
# As nosandbox cases my cause cache poluation, 'no-cache' is needed.
# nosandbox cases are non-hermetic, they may depend on undeclared source files.
# The most common error is undeclared-inclusion check error, you should clean
# the local cache to pass it.
def nosandbox_test(**args):
    tags = args.get("tags") or []
    tags.append("no-sandbox")
    tags.append("no-cache")
    args["tags"] = tags
    native.cc_test(**args)

def gen_cpp_code(name, elements_list, template_header, template, template_tail,
                 element_per_file = 20, suffix=".cpp"):
    bases = []
    base = 1

    for i in range(len(elements_list)):
        base = len(elements_list[i]) * base

    base_tmp = base
    for i in range(len(elements_list)):
        base_tmp = base_tmp // len(elements_list[i])
        bases.append(base_tmp)

    files = []
    current = 0
    count = 0
    current_str = template_header
    for i in range(base):
        replace_elements_list = []
        num = i
        for j in range(len(bases)):
            replace_elements_list.append(elements_list[j][num // bases[j]])
            num %= bases[j]
        # for all permutations here

        if type(replace_elements_list[0]) == "tuple":
            replace_elements_list = replace_elements_list[0]
        else:
            replace_elements_list = tuple(replace_elements_list)
        current_str += template.format(*replace_elements_list)
        current += 1
        if current == element_per_file or i == base - 1:
            cpp_name = name + "_" + str(count)
            count += 1
            file_name = cpp_name + suffix
            content = current_str + template_tail
            native.genrule(
                name = cpp_name,
                srcs = [],
                outs = [file_name],
                cmd = "cat > $@  << 'EOF'\n" + content + "EOF",
            )
            current = 0
            current_str = template_header
            files.append(cpp_name)

    native.filegroup(
        name = name,
        srcs = files
    )

def gen_single_cpp_code(name, elements_list, template):
    bases = []
    base = 1

    for i in range(len(elements_list)):
        base = len(elements_list[i]) * base

    base_tmp = base
    for i in range(len(elements_list)):
        base_tmp = base_tmp // len(elements_list[i])
        bases.append(base_tmp)

    content = ''
    for i in range(base):
        replace_elements_list = []
        num = i
        for j in range(len(bases)):
            replace_elements_list.append(elements_list[j][num // bases[j]])
            num %= bases[j]
        content += template.format(*tuple(replace_elements_list))

    native.genrule(
        name = name + '_gen',
        srcs = [],
        outs = [name],
        cmd = "cat > $@  << 'EOF'\n" + content + "EOF",
    )


def cmake_library(name, srcs, cmake_cmd, hdrs, static_lib, shared_lib, thread_num = 96):
    hdrs = [ "install_root/include/" + hdr for hdr in hdrs ]
    outs = [] + hdrs
    local_static_lib = static_lib
    if local_static_lib:
        local_static_lib = "install_root/lib/" + static_lib
        outs.append(local_static_lib)

    local_shared_lib = shared_lib
    if shared_lib:
        local_shared_lib = "install_root/lib/" + shared_lib
        outs.append(local_shared_lib)

    cmd = "%s -DCMAKE_INSTALL_PREFIX:PATH=$(@D)/install_root/  && make -j%d install" % (cmake_cmd, thread_num)
    native.genrule(
        name = name + "_cmake",
        srcs = srcs,
        outs = outs,
        cmd = cmd,
    )
    native.cc_import(
        name = name + "_import",
        static_library = local_static_lib,
        shared_library = local_shared_lib,
    )
    native.cc_library(
        name = name,
        hdrs = hdrs,
        deps = [name + "_import"],
        strip_include_prefix = "install_root/include",
        visibility = ["//visibility:public"],
    )


def copy_target_to(name, to_copy, copy_name, dests = [], **kwargs):
    if dests:
        outs = [path + copy_name for path in dests]
        cmds = ["mkdir -p %s" % (dest) for dest in dests]
        cmd = "&&".join(cmds) + " && "
    else:
        outs = [copy_name]
        cmd = ""
    cmd += "for out in $(OUTS); do cp $(location %s) $$out; done" % to_copy
    native.genrule(
        name = name,
        srcs = [to_copy],
        outs = outs,
        cmd = cmd,
        **kwargs
    )



def _copy_targets_to_v2_impl(ctx):
    all_input_files = ctx.attr.filegroups[0].files.to_list()
    dest = ctx.attr.dests[0]
    all_outputs = []
    for f in all_input_files:
        out = ctx.actions.declare_file('%s/%s' % (dest, f.short_path))
        all_outputs += [out]
        ctx.actions.run_shell(
            outputs=[out],
            inputs=depset([f]),
            arguments=[f.path, dest, out.path],
            # This is what we're all about here. Just a simple 'cp' command.
            # Copy the input to CWD/f.basename, where CWD is the package where
            # the copy_filegroups_to_this_package rule is invoked.
            # (To be clear, the files aren't copied right to where your BUILD
            # file sits in source control. They are copied to the 'shadow tree'
            # parallel location under `bazel info bazel-bin`)
            command="mkdir -p $2 && cp $1 $3")

    # Small sanity check
    if len(all_input_files) != len(all_outputs):
        fail("Output count should be 1-to-1 with input count.")

    return [
        DefaultInfo(
            files=depset(all_outputs),
            runfiles=ctx.runfiles(files=all_outputs))
    ]


copy_targets_to = rule(
    implementation=_copy_targets_to_v2_impl,
    attrs={
        "filegroups": attr.label_list(),
        "dests": attr.string_list(),
    },
)

def ref_library(name, dests, **kwargs):
    ref_name = name + "_ref"
    so_name = "lib%s.so" % name
    so_tmp_name = "lib%stest.so" % name
    native.config_setting(
        name = "clang_build",
        values = {"define": "compiler_type=clang++"},
    )
    native.cc_binary(
        name = so_tmp_name,
        srcs = [name],
        copts = select({
        ":clang_build": [],
        "//conditions:default": ["-fno-gnu-unique",],
        }),
        linkshared = 1,
    )
    copy_target_to(
        name = ref_name,
        to_copy = so_tmp_name,
        copy_name = so_name,
        dests = dests,
        **kwargs
    )

def test_cc_plugin_shared_library(name,
                                  copy_dests,
                                  preloaded_deps,
                                  static_deps = [],
                                  **kwargs):
    library_name = name + "_cc"
    native.cc_library(
        name = library_name,
        deps = preloaded_deps + static_deps,
        **kwargs
    )
    so_name = name + ".so"
    cc_shared_library(
        name = so_name,
        roots = [library_name],
        preloaded_deps = preloaded_deps,
        static_deps= static_deps,
    )
    copy_target_to(
        name = name + "_testdata",
        to_copy = so_name,
        copy_name = "lib%s.so" % name,
        dests = copy_dests,
        visibility = ["//visibility:public"],
    )


def bc_code_impl(ctx):
    args = [
        '-emit-llvm',
    ]
    includes = []
    includes.extend(ctx.attr.includes)
    for dep in ctx.attr.deps:
        includes.extend(dep[CcInfo].compilation_context.includes.to_list())
    dep_files = _get_transitive_headers(ctx.files.srcs, ctx.attr.deps)
    args += ["-I" + i for i in includes]
    args += ["-c", ctx.files.srcs[0].path]
    args += ["-std=c++17", "-O2"]
    args += ["-D_GLIBCXX_USE_CXX11_ABI=" + str(ctx.attr.new_abi)]
    args += ["-o", ctx.outputs.bc_out.path]
    ctx.actions.run(
        executable = ctx.attr.tool.files.to_list()[0].path + '/bin/clang++',
        arguments = args,
        inputs = depset([], transitive=[dep_files, ctx.attr.tool.files]),
        outputs = [ctx.outputs.bc_out],
        progress_message = "Compiling " + ctx.files.srcs[0].path,
        execution_requirements = {
            "no-remote-exec": "1",
        },
    )

bc_code = rule(
    attrs = {
        "deps": attr.label_list(
            allow_files = True,
            providers = [CcInfo],
        ),
        "srcs": attr.label(
            allow_files = True,
            mandatory = True),
        "new_abi": attr.int(),
        "tool": attr.label(
            allow_files = True,
            mandatory = True),
        "includes": attr.string_list(),
    },
    implementation = bc_code_impl,
    outputs = {"bc_out" : "%{name}.bc"},
)

def test_bc_code(name, srcs, deps, copy_name, dests = []):
    bc_code(name = name,
            new_abi = new_abi_flag(),
            tool = "//third_party/clang-11:clang-11-for-cava_bins",
            srcs = srcs,
            deps = deps)
    copy_target_to(
        name = name + "_testdata",
        to_copy = name,
        copy_name = copy_name,
        dests = dests
    )

def test_bc_code_multi(name, srcs, deps, copy_name, dests = [], includes = [], **kwargs):
    cc_concat_cpp(name + '_concat', srcs)
    bc_code(name = name,
            new_abi = new_abi_flag(),
            tool = "//third_party/clang-11:clang-11-for-cava_bins",
            includes = includes,
            srcs = name + '_concat',
            deps = deps)
    copy_target_to(
        name = name + "_testdata",
        to_copy = name,
        copy_name = copy_name,
        dests = dests,
        **kwargs
    )

def cc_static_library(name, srcs):
    out = "lib" + name + ".a"
    native.genrule(
        name = name,
        srcs = srcs,
        outs = [out],
        cmd = "ar rc " + out + " ".join(["$(locations %s)" % src for src in srcs]),
    )

def cc_concat_cpp(name, srcs):
    out = "tmp_{}_out.cpp".format(name)
    native.genrule(
        name = name,
        srcs = srcs,
        outs = [out],
        cmd = "cat {} > $@".format(" ".join(["$(locations %s)" % src for src in srcs]))
    )


def _get_transitive_headers(hdrs, deps):
    return depset(
        hdrs,
        transitive = [dep[CcInfo].compilation_context.headers for dep in deps],
    )

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

def _remap(remap_paths, path):
    """If path starts with a key in remap_paths, rewrite it."""
    for prefix, replacement in remap_paths.items():
        if path.startswith(prefix):
            return replacement + path[len(prefix):]
    return path

def _quote(filename, protect = "="):
    """Quote the filename, by escaping = by \\= and \\ by \\\\"""
    return filename.replace("\\", "\\\\").replace(protect, "\\" + protect)

def _hdrs_pkg_impl(ctx):
    output_file = ctx.outputs.out
    remap_paths = ctx.attr.remap_paths
    args = [
        "--root_directory=" + ctx.attr.package_base,
        "--output=" + output_file.path,
    ]
    file_inputs = _get_transitive_headers([], ctx.attr.srcs[:]).to_list()
    prefix = ctx.attr.prefix
    if prefix.startswith("/"):
        fail("The prefix attribute can not start with '/'")
    if not prefix.endswith("/"):
        prefix += "/"
    # remove duplicated file names
    file_set = {}
    for f in file_inputs:
        dpath = _dest_path(f, ctx.attr.strip_prefixes)
        dpath = _remap(remap_paths, dpath)
        if prefix:
            dpath = prefix + dpath
        file_set[dpath] = f.path
    entries = [
        ["file", dpath, spath, '', '', ''] for dpath, spath in file_set.items()
    ]

    args += ["--tar=" + f.path for f in ctx.files.deps]
    # touch mtime to avoid untar warnings
    args.append("--mtime=portable")
    if ctx.attr.extension:
        dotPos = ctx.attr.extension.find(".")
        if dotPos > 0:
            dotPos += 1
            args += ["--compression=%s" % ctx.attr.extension[dotPos:]]
        elif ctx.attr.extension == "tgz":
            args += ["--compression=gz"]

    arg_file = ctx.actions.declare_file(ctx.label.name + ".args")
    ctx.actions.write(arg_file, "\n".join(args))
    manifest_file = ctx.actions.declare_file(ctx.label.name + ".manifest")
    ctx.actions.write(manifest_file, json.encode(entries))
    ctx.actions.run(
        mnemonic = "PackageTar",
        progress_message = "Writing: %s" % output_file.path,
        inputs = file_inputs + ctx.files.deps + [arg_file, manifest_file],
        executable = ctx.executable.build_tar,
        arguments = [
            "@" + arg_file.path,
            "--manifest", manifest_file.path
        ],
        outputs = [output_file],
        use_default_shell_env = True,
    )

tar_filetype = [".tar", ".tar.gz", ".tgz", ".tar.xz", ".tar.bz2"]
hdrs_pkg_impl = rule(
    implementation = _hdrs_pkg_impl,
    attrs = {
        "package_base": attr.string(default = "./"),
        "srcs": attr.label_list(allow_files = True),
        "deps": attr.label_list(allow_files = tar_filetype),
        "strip_prefixes": attr.string_list(),
        "prefix": attr.string(default=""),
        "remap_paths": attr.string_dict(),
        "extension": attr.string(default = "tar"),
        "out": attr.output(),
        # Implicit dependencies.
        "build_tar": attr.label(
            default = Label("@rules_pkg//pkg/private:build_tar"),
            cfg = "exec",
            executable = True,
            allow_files = True,
        ),
    },
)

def hdrs_pkg(name, **kwargs):
    extension = kwargs.get("extension") or "tar"
    hdrs_pkg_impl(
        name = name,
        out = name + "." + extension,
        **kwargs,
    )

def _upload_pkg_impl(ctx):
    ctx.actions.expand_template(
        template = ctx.file._deploy_script,
        substitutions = {
            "{oss_prefix}" : ctx.attr.oss_prefix,
            "{pkg_prefix}" : ctx.attr.pkg_prefix,
        },
        output = ctx.outputs.executable,
        is_executable = True
    )
    return DefaultInfo(
        executable = ctx.outputs.executable,
        runfiles = ctx.runfiles(
            files = [ctx.file.target],
            symlinks = {"pkg.tar": ctx.file.target}))

_upload_pkg = rule(
    attrs = {
        "target": attr.label(
            allow_single_file = True,
        ),
        "oss_prefix": attr.string(
            mandatory = True,
            doc = "upload to, ex. rtp_pkg",
        ),
        "pkg_prefix": attr.string(
            mandatory = True,
            doc = "ex. rtp_",
        ),
        "_deploy_script": attr.label(
            allow_single_file = True,
            default = "//bazel:upload_package.py",
        ),
    },
    implementation = _upload_pkg_impl,
    executable = True,
)

def upload_pkg(name, **kwargs):
    key = "tags"
    tags = kwargs.get(key) or []
    tags.extend(["manual"])
    kwargs.setdefault(key, tags)

    _upload_pkg( name = name, **kwargs)

def upload_wheel(name, src, dir, wheel_prefix):
    oss_path = "xxxx://invalid/%s/%s" % (dir, wheel_prefix) + "_$$(date '+%Y-%m-%d_%H_%M_%S')"
    native.genrule(
        name = name,
        srcs = [src],
        outs = ["tmp_wheel.whl"],        
        cmd = "bash -c 'set -xe;" +
            "mkdir tmp;" +
            "cp $(locations %s) tmp; " % (src) +
            "osscmd put $(locations %s) %s/$$(basename $(locations %s));" % (src, oss_path, src) +
            "mv tmp/$$(basename $(locations %s)) $(OUTS);" % (src) +
            "rm tmp -rf;" +
            "'",
        tags = [
            "local",
            "manual",
        ],
        visibility = ["//visibility:public"],
    )


def perf_test(group_name,
              suite_name,
              online_package,
              deps = [],
              data = [],
              tags = [],
              env = {}):
    full_suite_name = group_name + "_" + suite_name
    data_name = full_suite_name + "_data"
    native.filegroup(name = data_name,
                     srcs = native.glob([group_name + "/data/" + suite_name + "/**/*"]))
    native.py_test(
        name = full_suite_name,
        main = group_name + "/" + suite_name + ".py",
        srcs = [
            group_name + "/" + suite_name + ".py",
        ],
        data = [
            ":" + data_name,
            online_package,
        ] + data,
        env = env,
        deps = deps,
        tags = tags + ["manual"],
        timeout = "long",
        legacy_create_init=0,
    )
    return full_suite_name

def rtp_perf_test(group_name, suite_name, data=[], tags=[]):
    deps = [
        "//turing_local_debug:serverlib",
    ]
    return perf_test(group_name, suite_name,
                     online_package="//package/rtp/online:install",
                     deps=deps + [
                     requirement("pytest"),
                     requirement("pytest-xdist"),
                     requirement("flatbuffers"),
                     ],
                     data=data,
                     env={'SUEZOPS_PLATFORM':'rtp'},
                     tags=tags)

def be_perf_test(group_name, suite_name, data=[], tags=[]):
    deps = [
        "//turing_local_debug:serverlib",
    ]
    return perf_test(group_name, suite_name,
                     online_package="//package/be/test:install",
                     deps=deps + [
                     requirement("pytest"),
                     requirement("pytest-xdist"),
                     requirement("flatbuffers"),
                     ],
                     data=data,
                     env={'SUEZOPS_PLATFORM':'rtp'},
                     tags=tags)

def genshared(name, lib):
    return native.genrule(
        name = name,
        srcs = [lib],
        outs = [lib.split("/")[-1]],
        cmd = "ld.gold $(SRCS) -shared -o $@"
    )
