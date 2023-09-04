load('//bazel:bundle.bzl', 'bundle_files')
genrule(
    name='git_commit_version',
    srcs=[],
    outs=['git_commit'],
    cmd='cat bazel-out/stable-status.txt | grep -E "GIT|USER_NAME" > $(OUTS)',
    stamp=1
)
genrule(
    name='git_commit_version_log_latest',
    srcs=[':git_log.py', ':git_commit_version'],
    outs=['git_commit_log'],
    cmd='python3 $(location git_log.py) -o $(OUTS)'
)
bundle_files(
    name='commit_version_file',
    prefix='usr/local/etc',
    srcs=[':git_commit_version', ':git_commit_version_log_latest'],
    visibility=['//visibility:public']
)
genrule(
    name='git_brief_commit_version',
    srcs=[],
    outs=['commit'],
    cmd='cat bazel-out/stable-status.txt | grep -E "GIT" |awk \'{print $$2}\' > $(OUTS)',
    stamp=1
)
