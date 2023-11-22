package(default_visibility=['//aios/filesystem/fslib:__subpackages__'])
config_h_content_template = '//FslibSignature.h\n#pragma once\n#define __FSLIB_VERSION "%(fslib_version)s"\n#define __FSLIB_GIT_COMMIT R"===(%(fslib_git_commit)s)==="\n#define __FSLIB_GIT_STATUS R"===(%(fslib_git_status)s)==="\n#define __FSLIB_BUILD_INFO "%(fslib_build_info)s"\n'
config_headers = ['config.h', 'Signature.h']
[
    genrule(
        name=header.replace('/', '_').replace('.', '_'),
        srcs=[],
        outs=[header],
        cmd=(("cat > $@ <<'EOF'\n" + config_h_content_template) + 'EOF')
    ) for header in config_headers
]
cc_library(
    name='cc_fslib_config_h', hdrs=config_headers, include_prefix='fslib'
)
cc_library(
    name='common',
    hdrs=[
        'common.h', 'common/common_define.h', 'common/common_type.h', 'fslib.h'
    ],
    include_prefix='fslib',
    deps=['//aios/filesystem/fslib/fslib/fs:header']
)
