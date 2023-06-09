load('@bazel_tools//tools/build_defs/repo:http.bzl', 'http_archive')
load(
    '@bazel_tools//tools/jdk:local_java_repository.bzl', 'local_java_repository'
)
load('//bazel:platform_http.bzl', 'platform_http_archive')
DEFAULT_JVM_EXTERNAL_TAG = '3.3'
RULES_JVM_EXTERNAL_SHA = 'd85951a92c0908c80bd8551002d66cb23c3434409c814179c0ff026b53544dab'
BUILD_PROTO_MESSAGE_SHA = '50b79faec3c4154bed274371de5678b221165e38ab59c6167cc94b922d9d9152'
EXTERNAL_DIR = '/external/'


def clean_dep(dep):
    return str(Label(dep))


def aios_workspace():
    native.local_repository(
        name='suez_turing.turbojet.turing_ops_util',
        path=(EXTERNAL_DIR + 'suez_turing.turbojet.turing_ops_util')
    )
    native.local_repository(name='swift', path=(EXTERNAL_DIR + 'swift'))
    native.local_repository(name='cm2', path=(EXTERNAL_DIR + 'cm2'))
    native.local_repository(
        name='iquan_client', path=(EXTERNAL_DIR + 'iquan_client')
    )
    native.local_repository(name='pip', path=(EXTERNAL_DIR + 'pip'))
    native.local_repository(name='platforms', path=(EXTERNAL_DIR + 'platforms'))
    local_java_repository(
        name='alijdk', version='8', java_home='/opt/taobao/java'
    )
    native.local_repository(
        name='rules_python', path=(EXTERNAL_DIR + 'rules_python')
    )
    native.local_repository(
        name='rules_foreign_cc', path=(EXTERNAL_DIR + 'rules_foreign_cc')
    )
    native.local_repository(name='rt_libs', path=(EXTERNAL_DIR + 'rt_libs'))
    native.local_repository(name='jvm_libs', path=(EXTERNAL_DIR + 'jvm_libs'))
    native.local_repository(name='cppjieba', path=(EXTERNAL_DIR + 'cppjieba'))
    native.local_repository(
        name='rdkafkacpp', path=(EXTERNAL_DIR + 'rdkafkacpp')
    )
    native.local_repository(name='rules_cc', path=(EXTERNAL_DIR + 'rules_cc'))
    native.local_repository(
        name='io_bazel_rules_docker',
        path=(EXTERNAL_DIR + 'io_bazel_rules_docker')
    )
    native.local_repository(
        name='io_bazel_rules_closure',
        path=(EXTERNAL_DIR + 'io_bazel_rules_closure')
    )
    skylib_version = '1.0.3'
    native.local_repository(
        name='bazel_skylib', path=(EXTERNAL_DIR + 'bazel_skylib')
    )
    native.local_repository(
        name='com_google_googletest',
        path=(EXTERNAL_DIR + 'com_google_googletest')
    )
    native.local_repository(name='aliorc', path=(EXTERNAL_DIR + 'aliorc'))
    native.local_repository(name='grpc', path=(EXTERNAL_DIR + 'grpc'))
    tensorflow_commit = '0a261d84757b54f435f77769bb9451944014a162'
    tensorflow_prefix = 'tensorflow-{}-{}'.format(
        tensorflow_commit, tensorflow_commit
    )
    tensorflow_sha256 = 'dcb841ae506e616733d90b8432d300ccfe9ac12adbdf58eaf8469a5f0822515d'
    native.local_repository(
        name='org_tensorflow', path=(EXTERNAL_DIR + 'org_tensorflow')
    )
    aicompiler_commit = 'd893ddbd2b9fe0910c9ce58b1c05a9a9e27b0066'
    native.local_repository(
        name='zookeeper-package', path=(EXTERNAL_DIR + 'zookeeper-package')
    )
    native.local_repository(
        name='mxml-package', path=(EXTERNAL_DIR + 'mxml-package')
    )
    native.local_repository(name='curl', path=(EXTERNAL_DIR + 'curl'))
    native.local_repository(name='zstd', path=(EXTERNAL_DIR + 'zstd'))
    native.local_repository(name='lz4', path=(EXTERNAL_DIR + 'lz4'))
    native.local_repository(
        name='tbb-package', path=(EXTERNAL_DIR + 'tbb-package')
    )
    native.local_repository(
        name='tbb-devel-package', path=(EXTERNAL_DIR + 'tbb-devel-package')
    )
    native.local_repository(name='hiredis', path=(EXTERNAL_DIR + 'hiredis'))
    native.local_repository(name='snappy', path=(EXTERNAL_DIR + 'snappy'))
    native.local_repository(name='rapidjson', path=(EXTERNAL_DIR + 'rapidjson'))
    native.local_repository(name='llvm', path=(EXTERNAL_DIR + 'llvm'))
    native.local_repository(name='proxima', path=(EXTERNAL_DIR + 'proxima'))
    native.local_repository(name='hdfs-cdh', path=(EXTERNAL_DIR + 'hdfs-cdh'))
    native.local_repository(name='tvm', path=(EXTERNAL_DIR + 'tvm'))
    native.local_repository(
        name='elfutils-libelf-devel',
        path=(EXTERNAL_DIR + 'elfutils-libelf-devel')
    )
    native.local_repository(
        name='elfutils-libelf-lib', path=(EXTERNAL_DIR + 'elfutils-libelf-lib')
    )
    native.local_repository(name='boost', path=(EXTERNAL_DIR + 'boost'))
    native.local_repository(name='rules_pkg', path=(EXTERNAL_DIR + 'rules_pkg'))
    native.local_repository(
        name='com_google_absl', path=(EXTERNAL_DIR + 'com_google_absl')
    )
    mjbots_deps_commit = 'dea4292627409f102a6314fce231ade4a0c342d2'
    native.local_repository(
        name='com_github_mjbots_bazel_deps',
        path=(EXTERNAL_DIR + 'com_github_mjbots_bazel_deps')
    )
    native.local_repository(name='fmt', path=(EXTERNAL_DIR + 'fmt'))


def _maybe(repo_rule, name, **kwargs):
    if (not native.existing_rule(name)):
        repo_rule(name=name, **kwargs)


def bazel_diff_dependencies(
    rules_jvm_external_tag=DEFAULT_JVM_EXTERNAL_TAG,
    rules_jvm_external_sha=RULES_JVM_EXTERNAL_SHA
):
    _maybe(
        http_archive,
        name='bazel_skylib',
        urls=[
            'https://github.com/bazelbuild/bazel-skylib/releases/download/1.0.2/bazel-skylib-1.0.2.tar.gz',
            'https://github.com/bazelbuild/bazel-skylib/releases/download/1.0.2/bazel-skylib-1.0.2.tar.gz'
        ],
        sha256='97e70364e9249702246c0e9444bccdc4b847bed1eb03c5a3ece4f83dfe6abc44'
    )
    _maybe(
        http_archive,
        name='rules_proto',
        sha256='602e7161d9195e50246177e7c55b2f39950a9cf7366f74ed5f22fd45750cd208',
        strip_prefix='rules_proto-97d8af4dc474595af3900dd85cb3a29ad28cc313',
        urls=[
            'https://github.com/bazelbuild/rules_proto/archive/97d8af4dc474595af3900dd85cb3a29ad28cc313.tar.gz',
            'https://github.com/bazelbuild/rules_proto/archive/97d8af4dc474595af3900dd85cb3a29ad28cc313.tar.gz'
        ]
    )
    _maybe(
        http_archive,
        name='rules_jvm_external',
        strip_prefix=('rules_jvm_external-%s' % rules_jvm_external_tag),
        sha256=rules_jvm_external_sha,
        url=(
            'https://github.com/bazelbuild/rules_jvm_external/archive/%s.zip' %
            rules_jvm_external_tag
        )
    )
