load('//aios/apps/facility/build_service:defs.bzl', 'bs_lua_path_id')
filegroup(
    name='basic_lua',
    srcs=glob(['*.graph', '*.flow', '*.el']),
    visibility=['//visibility:public']
)
filegroup(
    name='batch_build_lua',
    srcs=glob(['BatchBuild/*']),
    visibility=['//visibility:public']
)
filegroup(
    name='batch_build_v2_lua',
    srcs=glob(['BatchBuildV2/*']),
    visibility=['//visibility:public']
)
filegroup(
    name='update2add_lua',
    srcs=glob(['Update2Add/*']),
    visibility=['//visibility:public']
)
filegroup(
    name='high_quality_update2add_lua',
    srcs=glob(['HighQualityUpdate2Add/*']),
    visibility=['//visibility:public']
)
genrule(
    name='bs_lua_script',
    srcs=[
        ':basic_lua', ':batch_build_lua', ':batch_build_v2_lua',
        ':update2add_lua', ':high_quality_update2add_lua'
    ],
    outs=[(bs_lua_path_id + '.zip')],
    cmd='echo $(locations :basic_lua)|tr -s \' \' \'\n\'|awk -F\'/\' \'{print $$NF"="$$0}\' > lua_file &&            echo $(locations :batch_build_lua)|tr -s \' \' \'\n\'|awk -F\'/\' \'{print "BatchBuild/"$$NF"="$$0}\' > lua_file2 &&            echo $(locations :batch_build_v2_lua)|tr -s \' \' \'\n\'|awk -F\'/\' \'{print "BatchBuildV2/"$$NF"="$$0}\' > lua_file3 &&            echo $(locations :update2add_lua)|tr -s \' \' \'\n\'|awk -F\'/\' \'{print "Update2Add/"$$NF"="$$0}\' > lua_file4 &&            echo $(locations :high_quality_update2add_lua)|tr -s \' \' \'\n\'|awk -F\'/\' \'{print "HighQualityUpdate2Add/"$$NF"="$$0}\' > lua_file5 &&            inputs=`cat lua_file lua_file2 lua_file3 lua_file4 lua_file5` && echo $$inputs && $(location @bazel_tools//tools/zip:zipper) c $@ $$inputs',
    tools=['@bazel_tools//tools/zip:zipper'],
    visibility=['//visibility:public']
)
