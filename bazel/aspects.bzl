# Copyright 2017 GRAIL, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Compilation database generation Bazel rules.

compilation_database will generate a compile_commands.json file for the
given targets. This approach uses the aspects feature of bazel.

An alternative approach is the one used by the kythe project using
(experimental) action listeners.
https://github.com/google/kythe/blob/master/tools/cpp/generate_compilation_database.sh
"""

load("@bazel_tools//tools/cpp:toolchain_utils.bzl", "find_cpp_toolchain")
load(
    "@bazel_tools//tools/build_defs/cc:action_names.bzl",
    "CPP_COMPILE_ACTION_NAME",
    "C_COMPILE_ACTION_NAME",
    "OBJCPP_COMPILE_ACTION_NAME",
    "OBJC_COMPILE_ACTION_NAME",
)

CompilationAspect = provider()

_cpp_extensions = [
    "cc",
    "cpp",
    "cxx",
]

_cc_rules = [
    "cc_library",
    "cc_binary",
    "cc_test",
    "cc_inc_library",
    "cc_proto_library",
]

_objc_rules = [
    "objc_library",
    "objc_binary",
]

_all_rules = _cc_rules + _objc_rules

def _compilation_db_json(compilation_db):
    # Return a JSON string for the compilation db entries.

    entries = [entry.to_json() for entry in compilation_db]
    return ",\n".join(entries)

def _is_cpp_target(srcs):
    return any([src.extension in _cpp_extensions for src in srcs])

def _is_objcpp_target(srcs):
    for src in srcs:
        if src.extension == "mm":
            return True

    return False

def _sources(target, ctx):
    srcs = []
    if "srcs" in dir(ctx.rule.attr):
        srcs += [f for src in ctx.rule.attr.srcs for f in src.files.to_list()]
    if "hdrs" in dir(ctx.rule.attr):
        srcs += [f for src in ctx.rule.attr.hdrs for f in src.files.to_list()]

    if ctx.rule.kind == "cc_proto_library":
        srcs += [f for f in target.files.to_list() if f.extension in ["h", "cc"]]

    return srcs

# Function copied from https://gist.github.com/oquenchil/7e2c2bd761aa1341b458cc25608da50c
def get_compile_flags(dep):
    options = []
    compilation_context = dep[CcInfo].compilation_context
    for define in compilation_context.defines.to_list():
        options.append("-D\"{}\"".format(define))

    for system_include in compilation_context.system_includes.to_list():
        if len(system_include) == 0:
            system_include = "."
        options.append("-isystem {}".format(system_include))

    for include in compilation_context.includes.to_list():
        if len(include) == 0:
            include = "."
        options.append("-I {}".format(include))

    for quote_include in compilation_context.quote_includes.to_list():
        if len(quote_include) == 0:
            quote_include = "."
        options.append("-iquote {}".format(quote_include))

    return options

def _cc_compiler_info(ctx, target, srcs, feature_configuration, cc_toolchain):
    compile_variables = None
    compiler_options = None
    compiler = None
    compile_flags = None
    force_language_mode_option = ""

    # This is useful for compiling .h headers as C++ code.
    if _is_cpp_target(srcs):
        compile_variables = cc_common.create_compile_variables(
            feature_configuration = feature_configuration,
            cc_toolchain = cc_toolchain,
            user_compile_flags = ctx.fragments.cpp.cxxopts +
                                 ctx.fragments.cpp.copts,
            add_legacy_cxx_options = True,
        )
        compiler_options = cc_common.get_memory_inefficient_command_line(
            feature_configuration = feature_configuration,
            action_name = CPP_COMPILE_ACTION_NAME,
            variables = compile_variables,
        )
        force_language_mode_option = " -x c++ -std=c++20"
    else:
        compile_variables = cc_common.create_compile_variables(
            feature_configuration = feature_configuration,
            cc_toolchain = cc_toolchain,
            user_compile_flags = ctx.fragments.cpp.copts,
        )
        compiler_options = cc_common.get_memory_inefficient_command_line(
            feature_configuration = feature_configuration,
            action_name = C_COMPILE_ACTION_NAME,
            variables = compile_variables,
        )

    compiler = str(
        cc_common.get_tool_for_action(
            feature_configuration = feature_configuration,
            action_name = C_COMPILE_ACTION_NAME,
        ),
    )

    compile_flags = (compiler_options +
                     get_compile_flags(target) +
                     (ctx.rule.attr.copts if "copts" in dir(ctx.rule.attr) else []))

    return struct(
        compile_variables = compile_variables,
        compiler_options = compiler_options,
        compiler = compiler,
        compile_flags = compile_flags,
        force_language_mode_option = force_language_mode_option,
    )

def _objc_compiler_info(ctx, target, srcs, feature_configuration, cc_toolchain):
    compile_variables = None
    compiler_options = None
    compiler = None
    compile_flags = None
    force_language_mode_option = ""

    # This is useful for compiling .h headers as C++ code.
    if _is_objcpp_target(srcs):
        compile_variables = cc_common.create_compile_variables(
            feature_configuration = feature_configuration,
            cc_toolchain = cc_toolchain,
            user_compile_flags = ctx.fragments.objc.copts,
            add_legacy_cxx_options = True,
        )
        compiler_options = cc_common.get_memory_inefficient_command_line(
            feature_configuration = feature_configuration,
            action_name = OBJCPP_COMPILE_ACTION_NAME,
            variables = compile_variables,
        )
        force_language_mode_option = " -x objective-c++"
    else:
        compile_variables = cc_common.create_compile_variables(
            feature_configuration = feature_configuration,
            cc_toolchain = cc_toolchain,
            user_compile_flags = ctx.fragments.objc.copts,
        )
        compiler_options = cc_common.get_memory_inefficient_command_line(
            feature_configuration = feature_configuration,
            action_name = OBJC_COMPILE_ACTION_NAME,
            variables = compile_variables,
        )
        force_language_mode_option = " -x objective-c"

    compiler = str(
        cc_common.get_tool_for_action(
            feature_configuration = feature_configuration,
            action_name = OBJC_COMPILE_ACTION_NAME,
        ),
    )

    defines = ["-D\"{}\"".format(val) for val in target.objc.define.to_list()]
    includes = ["-I{}".format(val) for val in target.objc.include.to_list()]
    system_includes = ["-isystem {}".format(val) for val in target.objc.include_system.to_list()]
    iquotes = ["-iquote {}".format(val) for val in target.objc.iquote.to_list()]
    frameworks = (["-F {}/..".format(val) for val in target.objc.static_framework_paths.to_list()] +
                  ["-F {}/..".format(val) for val in target.objc.dynamic_framework_paths.to_list()] +
                  ["-F {}/..".format(val) for val in target.objc.framework_search_path_only.to_list()])

    xcode_config = ctx.attr._xcode_config[apple_common.XcodeVersionConfig]

    sdk_version = xcode_config.sdk_version_for_platform(ctx.fragments.apple.single_arch_platform)
    apple_env = apple_common.target_apple_env(xcode_config, ctx.fragments.apple.single_arch_platform)
    sdk_platform = apple_env["APPLE_SDK_PLATFORM"]

    # FIXME is there any way of getting the SDKROOT value here? The only thing that seems to know about it is
    # XcodeLocalEnvProvider, but I can't seem to find a way to access that
    platform_root = "/Applications/Xcode.app/Contents/Developer/Platforms/{platform}.platform".format(platform = sdk_platform)
    sdk_root = "/Applications/Xcode.app/Contents/Developer/Platforms/{platform}.platform/Developer/SDKs/{platform}{version}.sdk".format(platform = sdk_platform, version = sdk_version)

    compile_flags = (compiler_options +
                     ["-isysroot {}".format(sdk_root)] +
                     ["-F {}/System/Library/Frameworks".format(sdk_root)] +
                     ["-F {}/Developer/Library/Frameworks".format(platform_root)] +
                     # FIXME this needs to be done per-file to be fully correct
                     ["-fobjc-arc"] +
                     defines +
                     includes +
                     iquotes +
                     system_includes +
                     frameworks +
                     (ctx.rule.attr.copts if "copts" in dir(ctx.rule.attr) else []))

    return struct(
        compile_variables = compile_variables,
        compiler_options = compiler_options,
        compiler = compiler,
        compile_flags = compile_flags,
        force_language_mode_option = force_language_mode_option,
    )

def _compilation_database_aspect_impl(target, ctx):
    # Write the compile commands for this target to a file, and return
    # the commands for the transitive closure.

    # We support only these rule kinds.
    if ctx.rule.kind not in _all_rules:
        return []

    compilation_db = []

    cc_toolchain = find_cpp_toolchain(ctx)
    feature_configuration = cc_common.configure_features(
        ctx = ctx,
        cc_toolchain = cc_toolchain,
        requested_features = ctx.features,
        unsupported_features = ctx.disabled_features,
    )

    srcs = _sources(target, ctx)

    compiler_info = None

    if ctx.rule.kind in _cc_rules:
        compiler_info = _cc_compiler_info(ctx, target, srcs, feature_configuration, cc_toolchain)
    else:
        compiler_info = _objc_compiler_info(ctx, target, srcs, feature_configuration, cc_toolchain)

    compile_command = compiler_info.compiler + " " + " ".join(compiler_info.compile_flags) + compiler_info.force_language_mode_option

    for src in srcs:
        if src.path.endswith('.h'):
            command_for_file = compile_command + " -c -x c++-header -std=c++20 " + src.path
        else:
            command_for_file = compile_command + " -c " + src.path
        exec_root_marker = "__EXEC_ROOT__"
        compilation_db.append(
            struct(command = command_for_file, directory = exec_root_marker, file = src.path),
        )

    # Write the commands for this target.
    compdb_file = ctx.actions.declare_file(ctx.label.name + ".compile_commands.json")
    ctx.actions.write(
        content = _compilation_db_json(compilation_db),
        output = compdb_file,
    )

    # Collect all transitive dependencies.
    transitive_compilation_db = []
    all_compdb_files = []
    for dep in ctx.rule.attr.deps:
        if CompilationAspect not in dep:
            continue
        transitive_compilation_db.append(dep[CompilationAspect].compilation_db)
        all_compdb_files.append(dep[OutputGroupInfo].compdb_files)

    compilation_db = depset(compilation_db, transitive = transitive_compilation_db)
    all_compdb_files = depset([compdb_file], transitive = all_compdb_files)

    return [
        CompilationAspect(compilation_db = compilation_db),
        OutputGroupInfo(
            compdb_files = all_compdb_files,
            header_files = target[CcInfo].compilation_context.headers,
        ),
    ]

compilation_database_aspect = aspect(
    attr_aspects = ["deps"],
    attrs = {
        "_cc_toolchain": attr.label(
            default = Label("@bazel_tools//tools/cpp:current_cc_toolchain"),
        ),
        "_xcode_config": attr.label(default = Label("@bazel_tools//tools/osx:current_xcode_config")),
    },
    fragments = ["cpp", "objc", "apple"],
    required_aspect_providers = [CompilationAspect],
    toolchains = ["@bazel_tools//tools/cpp:toolchain_type"],
    implementation = _compilation_database_aspect_impl,
)

def _compilation_database_impl(ctx):
    # Generates a single compile_commands.json file with the
    # transitive depset of specified targets.

    if ctx.attr.disable:
        ctx.actions.write(output = ctx.outputs.filename, content = "[]\n")
        return

    compilation_db = []
    all_headers = []
    for target in ctx.attr.targets:
        compilation_db.append(target[CompilationAspect].compilation_db)
        all_headers.append(target[OutputGroupInfo].header_files)

    compilation_db = depset(transitive = compilation_db)
    all_headers = depset(transitive = all_headers)

    content = "[\n" + _compilation_db_json(compilation_db.to_list()) + "\n]\n"
    content = content.replace("__EXEC_ROOT__", ctx.attr.exec_root)
    content = content.replace("-isysroot __BAZEL_XCODE_SDKROOT__", "")
    ctx.actions.write(output = ctx.outputs.filename, content = content)

    return [
        OutputGroupInfo(
            default = all_headers,
        ),
    ]

_compilation_database = rule(
    attrs = {
        "targets": attr.label_list(
            aspects = [compilation_database_aspect],
            doc = "List of all cc targets which should be included.",
        ),
        "exec_root": attr.string(
            default = "__EXEC_ROOT__",
            doc = "Execution root of Bazel as returned by 'bazel info execution_root'.",
        ),
        "disable": attr.bool(
            default = False,
            doc = ("Makes this operation a no-op; useful in combination with a 'select' " +
                   "for platforms where the internals of this rule are not properly " +
                   "supported."),
        ),
        "filename": attr.output(
            doc = "Name of the generated compilation database.",
        ),
    },
    implementation = _compilation_database_impl,
)

def compilation_database(**kwargs):
    _compilation_database(
        filename = kwargs.pop("filename", "compile_commands.json"),
        **kwargs,
    )
