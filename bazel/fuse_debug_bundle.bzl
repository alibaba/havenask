load("//bazel:bundle.bzl", "bundle_files", "bundle_tar", "BundleFilesInfo", "mapping_files")

BundleStripFilesInfo = provider( fields = [ "dest_src_map" ] )
BundleDebugFilesInfo = provider( fields = [ "dest_src_map" ] )
KeepSymbolInfo = provider( fields = [ "target" ] )

def _keep_symbol_collector_aspect_impl(target, ctx):
    # This function is executed for each dependency the aspect visits.
    collected = []
    attr = ctx.rule.attr
    if hasattr(attr, 'tags'):
        if 'fuse-keep-symbol' in attr.tags:
            collected.extend(target.files.to_list())

    def _travel_sub_attr(name):
        if not hasattr(attr, name):
            return
        sub_attr = getattr(attr, name)
        if type(sub_attr) == 'list':
            for e in sub_attr:
                if KeepSymbolInfo in e:
                    collected.extend(e[KeepSymbolInfo].target)
    _travel_sub_attr('deps')
    _travel_sub_attr('srcs')
    return [KeepSymbolInfo(target=collected)]

keep_symbol_collector_aspect = aspect(
    implementation = _keep_symbol_collector_aspect_impl,
    attr_aspects = ["deps", "srcs"],
)

def modify_bundle_impl(ctx):
    keep_symbols_set = [f for f in ctx.files.keep_symbols]
    original_src_map = {}
    final_src_map = {}
    final_dbg_map = {}
    for e in ctx.attr.deps:
        keep_symbols_set.extend(e[KeepSymbolInfo].target)
    for e in ctx.attr.deps:
        dest_src_map = e[BundleFilesInfo].dest_src_map
        for key in dest_src_map:
            obj = dest_src_map[key]
            if key in original_src_map:
                prev = original_src_map[key]
                if obj == prev:
                    continue
                else:
                    fail("key [{}] duplicated in bundle_files, previous [{}] current [{}]".format(key, prev, obj))
            original_src_map[key] = obj
            if obj in keep_symbols_set:
                raw_file = dest_src_map[key]
                strip_file = ctx.actions.declare_file(obj.path + ".strip")
                debug_file = ctx.actions.declare_file(obj.path + ".debug")

                raw_path = raw_file.path
                strip_path = strip_file.path
                debug_path = debug_file.path

                cmd_bash = """#!/bin/bash
set -e
chmod a+w {raw_path}
gdb-add-index {raw_path}
objcopy --only-keep-debug {raw_path} {debug_path}
strip -g {raw_path} -o {strip_path}
objcopy --add-gnu-debuglink={debug_path} {strip_path}
""".format(raw_path=raw_path, strip_path=strip_path, debug_path=debug_path)
                cmd_file = ctx.actions.declare_file(key + "_strip.sh")
                ctx.actions.write(cmd_file, cmd_bash)
                ctx.actions.run(
                    executable = "/bin/sh",
                    use_default_shell_env = True,
                    arguments = [cmd_file.path],
                    inputs = [raw_file, cmd_file],
                    outputs = [strip_file, debug_file],
                    progress_message = "Separating debug info..." + raw_path,
                    execution_requirements = {
                        "no-remote-exec": "1",
                    },
                )
                final_src_map[key] = strip_file
                final_dbg_map[key + ".debug"] = debug_file
            elif key.startswith("usr/local/bin/") and not obj.is_source and not key.endswith(".dwp") or obj.extension == "so":
                output_file = ctx.actions.declare_file(obj.path + ".strip")
                cmd_bash = "strip -g {input_path} -o {output_path}".format(
                    input_path=obj.path,
                    output_path=output_file.path)
                ctx.actions.run_shell(
                    inputs = [obj],
                    outputs = [output_file],
                    command = cmd_bash,
                    progress_message = "Stripping debug info..." + obj.path,
                    execution_requirements = {
                        "no-remote-exec": "1",
                    },
                    use_default_shell_env = True,
                )
                final_src_map[key] = output_file
            else:
                final_src_map[key] = obj
    return [
        BundleStripFilesInfo(
            dest_src_map = final_src_map,
        ),
        BundleDebugFilesInfo(
            dest_src_map = final_dbg_map,
        ),
    ]

modify_bundle = rule(
    attrs = {
        "deps": attr.label_list(
            allow_files = True,
            mandatory = True,
            aspects = [keep_symbol_collector_aspect]),
        "keep_symbols": attr.label_list(
            allow_files = True,
            mandatory = True),
    },
    implementation = modify_bundle_impl,
    provides = [BundleStripFilesInfo, BundleDebugFilesInfo],
)

def extract_bundle_files_impl(ctx):
    extract_map = {}
    if ctx.attr.extract_type == "debug":
        extract_map = ctx.attr.src[BundleDebugFilesInfo].dest_src_map
    elif ctx.attr.extract_type == "strip":
        extract_map = ctx.attr.src[BundleStripFilesInfo].dest_src_map
    else:
        fail("not supported mode [{}]".format(ctx.attr.extract_map))
    return [
        BundleFilesInfo(
            dest_src_map = extract_map,
        ),
    ]

extract_bundle_files = rule(
    attrs = {
        "src": attr.label(
            mandatory = True),
        "extract_type": attr.string(
            mandatory = True),
    },
    implementation = extract_bundle_files_impl,
    provides = [BundleFilesInfo],
)

def separate_bundle(name, deps, keep_symbols, strip_bundle, debug_bundle):
    modify_name = name + "_modify"
    modify_bundle(
        name = modify_name,
        deps = deps,
        keep_symbols = keep_symbols
    )
    extract_bundle_files(
        name = strip_bundle,
        src = modify_name,
        extract_type = "strip",
    )
    extract_bundle_files(
        name = debug_bundle,
        src = modify_name,
        extract_type = "debug",
    )


def _upload_tar(ctx, package_tar_file, oss_prefix, pkg_prefix, **kwargs):
    upload_result_file = ctx.actions.declare_file("upload_result.log")
    cmd_bash = """
osscmd multiupload {package_tar_path} {oss_prefix}/{pkg_prefix}_`date '+%Y%m%d%H%M%S'`_`md5sum {package_tar_path}|cut -d ' ' -f 1`.tar.gz > {upload_result_path}
    """.format(package_tar_path=package_tar_file.path,
               oss_prefix=oss_prefix,
               pkg_prefix=pkg_prefix,
               upload_result_path=upload_result_file.path
    )
    ctx.actions.run_shell(
        inputs = [package_tar_file],
        outputs = [upload_result_file],
        command = cmd_bash,
        execution_requirements = {
            "no-remote-exec": "1",
        },
        use_default_shell_env = True,
    )
    return upload_result_file

def _gen_download_file(ctx, upload_result_file, **kwargs):
    debug_symbol_url_file = ctx.actions.declare_file("debug_symbol_url.txt")
    download_debug_info_file = ctx.actions.declare_file("download_debug_info.sh")
    link_command="""result=""
function get_file_path() {
    build_id=`readelf -n $1 | grep 'Build ID' | awk '{print $3}' | tr -d '\\n'`
    first_two=`echo $build_id | head -c2`
    remain=`echo $build_id | tail -c+3`
    result="$first_two/$remain"
    result+=".debug"
}
gdb_debug_dir=`gdb -ex "show debug-file-directory" -ex "q" | grep "separate debug symbols are searched for" | awk -F'"' '{print $2}' | awk -F":" '{print $1}'`
echo $gdb_debug_dir
function process() {
    real_path=`realpath $1`
    echo $real_path
    get_file_path $real_path
    link_name=$gdb_debug_dir/.build-id/$result
    echo $link_name
    sudo mkdir -p `dirname $link_name`
    sudo ln -f -s $real_path $link_name
}
find $FILE_DIR -name "*.debug" -print0 | while read -d $'' file
do
    process "$file"
done
"""
    link_file = ctx.actions.declare_file("link_file.sh")
    ctx.actions.write(link_file, link_command, is_executable=True)

    cmd_bash = """
echo `tail -n 3 {upload_result_path} | grep "Object URL" | cut -d ' ' -f 4` > {debug_symbol_url_file}
    """.format(upload_result_path=upload_result_file.path,
               debug_symbol_url_file=debug_symbol_url_file.path)

    ctx.actions.run_shell(
        inputs = [upload_result_file],
        outputs = [debug_symbol_url_file],
        command = cmd_bash,
        execution_requirements = {
            "no-remote-exec": "1",
        },
        use_default_shell_env = True,
    )

    download_command = """#!/usr/bin/bash
FILE_DIR=`dirname $BASH_SOURCE[0]`
FILE_DIR=`realpath $FILE_DIR`
FILE_NAME=`tr -dc '[:alnum:]' </dev/urandom | head -c 13`".tar.gz"
PIGZ_OPTIONS=" -I pigz "
if ! command -v pigz &> /dev/null
then
    echo "pigz not exists, skip using pigz"
    PIGZ_OPTIONS=""
fi
wget `cat $FILE_DIR/{debug_symbol_url_path}` --backups 0 -O $FILE_DIR/$FILE_NAME
cd $FILE_DIR/../../../
tar $PIGZ_OPTIONS -xvf $FILE_DIR/$FILE_NAME
sh $FILE_DIR/link_file.sh
    """.format(debug_symbol_url_path=debug_symbol_url_file.basename)
    ctx.actions.write(download_debug_info_file, download_command, is_executable=True)

    return download_debug_info_file, link_file, debug_symbol_url_file


def _handle_debug_bundle_impl(ctx):
    mode = ctx.attr.mode
    name = ctx.attr.name
    strip_bundle = ctx.attr.strip_bundle
    debug_bundle = ctx.attr.debug_bundle
    debug_tar_file = ctx.attr.debug_tar[DefaultInfo].files.to_list()[0]

    final_srcs = []
    final_deps = []
    path_prefix = ""
    if mode == "upload":
        upload_result_file = _upload_tar(
            ctx = ctx,
            package_tar_file = debug_tar_file,
            oss_prefix = ctx.attr.oss_prefix,
            pkg_prefix = ctx.attr.pkg_prefix,
        )
        # TODO upload error check
        download_debug_info_file, link_file, debug_symbol_url_file = _gen_download_file(
            ctx = ctx,
            upload_result_file = upload_result_file,
        )
        final_srcs = [ download_debug_info_file, link_file, debug_symbol_url_file ]
        final_deps = [ strip_bundle, ]
        path_prefix = "/usr/local/bin"
    elif mode == "group":
        debug_tar_info_file = ctx.actions.declare_file( "debug_tar.list")
        ctx.actions.run_shell(
            inputs = [debug_tar_file],
            outputs = [debug_tar_info_file],
            command = "ls -lh {} > {}".format(debug_tar_file.path, debug_tar_info_file.path),
            progress_message = "Separating debug info to {} and {}".format(
                debug_tar_file.path, debug_tar_info_file.path),
            execution_requirements = {
                "no-remote-exec": "1",
            },
            use_default_shell_env = True,
        )
        final_srcs = [ debug_tar_info_file, ]
        final_deps = [ strip_bundle, ]
        path_prefix = "/usr/local/etc"
    else:
        fail("unsupported mode[{}], only support [upload, group]".format(mode))

    srcs = [f for f in final_srcs]
    dest_src_map = mapping_files(srcs, final_deps, path_prefix, "")
    return [
        BundleFilesInfo(
            dest_src_map = dest_src_map,
        ),
    ]

_handle_debug_bundle = rule(
    attrs = {
        "mode": attr.string(
            default = "full"
        ),
        "strip_bundle": attr.label(
            mandatory = True
        ),
        "debug_bundle": attr.label(
            mandatory = True
        ),
        "debug_tar": attr.label(
            mandatory = True
        ),
        "oss_prefix": attr.string(
            default = "xxxx://invalid/debug_info_pkg",
        ),
        "pkg_prefix": attr.string(
            default = "debug_info",
        ),
    },
    implementation = _handle_debug_bundle_impl,
    provides = [BundleFilesInfo],
)

def debug_info_version_file(name, debug_version_file):
    native.genrule(
        name = "debug_info_git_commit_version" + name,
        srcs = [],
        outs = ["debug_info_git_commit" + name],
        cmd = "cat bazel-out/stable-status.txt | grep -E \"GIT|USER_NAME\" > $(OUTS)",
        stamp = 1,
    )
    bundle_files(
        name = debug_version_file,
        prefix = "usr/local/etc/debug_version",
        srcs = [
            ":debug_info_git_commit_version" + name,
        ],
    )


def fuse_debug_bundle_files(name, deps, compress = True, keep_symbols = [], **kwargs):
    native.config_setting(
        name = "debug_info_group" + name,
        define_values = { "debug_info_mode": "group" }
    )
    native.config_setting(
        name = "debug_info_upload" + name,
        define_values = { "debug_info_mode": "upload" }
    )

    strip_bundle = name + "_strip"
    debug_bundle = name + "_debug_info"
    separate_bundle(name, deps, keep_symbols, strip_bundle, debug_bundle)

    debug_version = name + "_debug_version_file"
    debug_info_version_file(name, debug_version)

    debug_tar = name + "_debug_info_tar"
    bundle_tar(
        name = debug_tar,
        extension = "tar.gz" if compress else "tar",
        srcs = [
            debug_bundle,
            debug_version,
        ],
        tags = [
            "manual",
            "no-remote-exec",
        ],
    )

    mode = select({
        ":debug_info_group" + name: "group",
        ":debug_info_upload" + name: "upload",
        "//conditions:default": "invalid_opt",
    })
    _handle_debug_bundle(
        name = name + "_type_fuse",
        mode = mode,
        strip_bundle = strip_bundle,
        debug_bundle = debug_bundle,
        debug_tar = debug_tar,
        tags = [
            "manual",
            "no-remote-exec",
        ],
        **kwargs,
    )
    bundle_files(
        name = name + "_type_raw",
        deps = deps,
        srcs = [],
        tags = [
            "manual",
            "no-remote-exec",
        ],
    )

    native.alias(
        name = name,
        actual = select({
            ":debug_info_group" + name: name + "_type_fuse",
            ":debug_info_upload" + name: name + "_type_fuse",
            "//conditions:default": name + "_type_raw",
        })
    )


def gdb_add_index(src, dst):
    return native.genrule(
        name = dst,
        outs = [dst],
        srcs = [src],
        cmd = "set -x; cp $(SRCS) $(OUTS); chmod a+w $(OUTS); gdb-add-index $(OUTS); chmod a-w $(OUTS)",
        visibility = ["//visibility:public"],
        tags = ["manual"],
    )

def cc_binary_with_gdb_index(name, **kwargs):
    origin_name = name + ".origin"
    native.cc_binary(name=origin_name, **kwargs)
    return gdb_add_index(origin_name, name)


def strip_debug_info(name):
    stripped_name = name + ".strip"
    return native.genrule(
        name = stripped_name,
        outs = [stripped_name],
        srcs = [name],
        cmd = "set -x; strip $(SRCS) -o $(OUTS); chmod a-w $(OUTS)",
        visibility = ["//visibility:public"],
        tags = ["manual"],
    )

def cc_binary_with_stripped(name, **kwargs):
    native.cc_binary(name = name, **kwargs)
    return strip_debug_info(name)