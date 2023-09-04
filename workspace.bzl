load('@bazel_tools//tools/build_defs/repo:http.bzl', 'http_archive')
load(
    '@bazel_tools//tools/jdk:local_java_repository.bzl', 'local_java_repository'
)
load('//bazel:platform_http.bzl', 'platform_http_archive')
EXTERNAL_DIR = '/external/'


def clean_dep(dep):
    return str(Label(dep))


def aios_workspace():
    native.local_repository(
        name='hape_depends', path=(EXTERNAL_DIR + 'hape_depends')
    )
    native.local_repository(
        name='rules_jvm_external_deps',
        path=(EXTERNAL_DIR + 'rules_jvm_external_deps')
    )
    native.local_repository(
        name='rules_java', path=(EXTERNAL_DIR + 'rules_java')
    )
    native.local_repository(
        name='iquan_maven', path=(EXTERNAL_DIR + 'iquan_maven')
    )
    native.local_repository(
        name='bazel_skylib', path=(EXTERNAL_DIR + 'bazel_skylib')
    )
    native.local_repository(
        name='rules_proto', path=(EXTERNAL_DIR + 'rules_proto')
    )
    native.local_repository(name='pip', path=(EXTERNAL_DIR + 'pip'))
    native.local_repository(
        name='jsoncpp_git', path=(EXTERNAL_DIR + 'jsoncpp_git')
    )
    native.local_repository(
        name='com_github_nanopb_nanopb',
        path=(EXTERNAL_DIR + 'com_github_nanopb_nanopb')
    )
    native.local_repository(
        name='flatbuffers', path=(EXTERNAL_DIR + 'flatbuffers')
    )
    native.local_repository(
        name='six_archive', path=(EXTERNAL_DIR + 'six_archive')
    )
    native.local_repository(name='boringssl', path=(EXTERNAL_DIR + 'boringssl'))
    native.local_repository(
        name='zlib_archive', path=(EXTERNAL_DIR + 'zlib_archive')
    )
    native.local_repository(
        name='com_google_protobuf', path=(EXTERNAL_DIR + 'com_google_protobuf')
    )
    native.local_repository(name='platforms', path=(EXTERNAL_DIR + 'platforms'))
    native.local_repository(
        name='remote_java_tools_8', path=(EXTERNAL_DIR + 'remote_java_tools_8')
    )
    local_java_repository(
        name='alijdk', version='8', java_home='/opt/taobao/java'
    )
    native.local_repository(
        name='rules_python', path=(EXTERNAL_DIR + 'rules_python')
    )
    native.local_repository(
        name='rules_foreign_cc', path=(EXTERNAL_DIR + 'rules_foreign_cc')
    )
    native.local_repository(
        name='rules_jvm_external', path=(EXTERNAL_DIR + 'rules_jvm_external')
    )
    native.local_repository(
        name='rules_spring', path=(EXTERNAL_DIR + 'rules_spring')
    )
    native.local_repository(name='rt_libs', path=(EXTERNAL_DIR + 'rt_libs'))
    native.local_repository(name='jvm_libs', path=(EXTERNAL_DIR + 'jvm_libs'))
    native.local_repository(name='cppjieba', path=(EXTERNAL_DIR + 'cppjieba'))
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
        name='com_google_googletest',
        path=(EXTERNAL_DIR + 'com_google_googletest')
    )
    native.local_repository(
        name='com_google_benchmark',
        path=(EXTERNAL_DIR + 'com_google_benchmark')
    )
    native.local_repository(name='grpc', path=(EXTERNAL_DIR + 'grpc'))
    tensorflow_commit = '0da4c05d4f60c571d36a603b8921ab3b0d4f4aa9'
    tensorflow_prefix = 'tensorflow-{}-{}'.format(
        tensorflow_commit, tensorflow_commit
    )
    tensorflow_sha256 = 'd8ed06cca645520516fe346ffca1f59a37a731e94e4faafc77d4b3ac96f26f4e'
    native.local_repository(
        name='org_tensorflow', path=(EXTERNAL_DIR + 'org_tensorflow')
    )
    aicompiler_commit = 'd893ddbd2b9fe0910c9ce58b1c05a9a9e27b0066'
    native.local_repository(
        name='dadi-cache-sdk', path=(EXTERNAL_DIR + 'dadi-cache-sdk')
    )
    native.local_repository(
        name='mongo-cxx-driver', path=(EXTERNAL_DIR + 'mongo-cxx-driver')
    )
    native.local_repository(
        name='zookeeper-package', path=(EXTERNAL_DIR + 'zookeeper-package')
    )
    native.local_repository(
        name='mxml-package', path=(EXTERNAL_DIR + 'mxml-package')
    )
    native.local_repository(
        name='keycenter4c', path=(EXTERNAL_DIR + 'keycenter4c')
    )
    native.local_repository(
        name='openssl-devel', path=(EXTERNAL_DIR + 'openssl-devel')
    )
    native.local_repository(name='openssl', path=(EXTERNAL_DIR + 'openssl'))
    native.local_repository(
        name='openssl-libs', path=(EXTERNAL_DIR + 'openssl-libs')
    )
    native.local_repository(
        name='krb5-devel', path=(EXTERNAL_DIR + 'krb5-devel')
    )
    native.local_repository(
        name='libcom_err-devel', path=(EXTERNAL_DIR + 'libcom_err-devel')
    )
    native.local_repository(name='lua-devel', path=(EXTERNAL_DIR + 'lua-devel'))
    native.local_repository(name='lua', path=(EXTERNAL_DIR + 'lua'))
    native.local_repository(name='xz-devel', path=(EXTERNAL_DIR + 'xz-devel'))
    native.local_repository(name='xz-libs', path=(EXTERNAL_DIR + 'xz-libs'))
    native.local_repository(name='curl', path=(EXTERNAL_DIR + 'curl'))
    native.local_repository(
        name='readline_file', path=(EXTERNAL_DIR + 'readline_file')
    )
    native.local_repository(
        name='readline-devel_file', path=(EXTERNAL_DIR + 'readline-devel_file')
    )
    native.local_repository(name='zstd', path=(EXTERNAL_DIR + 'zstd'))
    native.local_repository(name='lz4', path=(EXTERNAL_DIR + 'lz4'))
    native.local_repository(
        name='mysql-connector-cpp', path=(EXTERNAL_DIR + 'mysql-connector-cpp')
    )
    native.local_repository(
        name='tbb-package', path=(EXTERNAL_DIR + 'tbb-package')
    )
    native.local_repository(
        name='tbb-devel-package', path=(EXTERNAL_DIR + 'tbb-devel-package')
    )
    native.local_repository(name='hiredis', path=(EXTERNAL_DIR + 'hiredis'))
    native.local_repository(
        name='gperftools', path=(EXTERNAL_DIR + 'gperftools')
    )
    native.local_repository(name='snappy', path=(EXTERNAL_DIR + 'snappy'))
    native.local_repository(name='rapidjson', path=(EXTERNAL_DIR + 'rapidjson'))
    native.local_repository(name='llvm', path=(EXTERNAL_DIR + 'llvm'))
    native.local_repository(name='proxima2', path=(EXTERNAL_DIR + 'proxima2'))
    native.local_repository(name='hdfs-cdh', path=(EXTERNAL_DIR + 'hdfs-cdh'))
    native.local_repository(
        name='uuid-devel', path=(EXTERNAL_DIR + 'uuid-devel')
    )
    native.local_repository(name='uuid', path=(EXTERNAL_DIR + 'uuid'))
    native.local_repository(
        name='elfutils-libelf-devel',
        path=(EXTERNAL_DIR + 'elfutils-libelf-devel')
    )
    native.local_repository(
        name='elfutils-libelf-lib', path=(EXTERNAL_DIR + 'elfutils-libelf-lib')
    )
    native.local_repository(name='libev', path=(EXTERNAL_DIR + 'libev'))
    native.local_repository(
        name='libev-devel', path=(EXTERNAL_DIR + 'libev-devel')
    )
    native.local_repository(name='boost', path=(EXTERNAL_DIR + 'boost'))
    native.local_repository(name='rules_pkg', path=(EXTERNAL_DIR + 'rules_pkg'))
    native.local_repository(
        name='bhclient_cpp', path=(EXTERNAL_DIR + 'bhclient_cpp')
    )
    native.local_repository(
        name='com_google_absl', path=(EXTERNAL_DIR + 'com_google_absl')
    )
    mjbots_deps_commit = 'dea4292627409f102a6314fce231ade4a0c342d2'
    native.local_repository(name='fmt', path=(EXTERNAL_DIR + 'fmt'))
    native.bind(name='grpc_cpp_plugin', actual='@grpc//:grpc_cpp_plugin')
    native.bind(
        name='protobuf_headers',
        actual='@com_google_protobuf//:protobuf_headers'
    )
    native.bind(name='libssl', actual='@boringssl//:ssl')
    native.bind(name='six', actual='@six_archive//:six')
    native.bind(name='grpc_python_plugin', actual='@grpc//:grpc_python_plugin')
    native.bind(name='nanopb', actual='@com_github_nanopb_nanopb//:nanopb')
    native.bind(
        name='protobuf_clib', actual='@com_google_protobuf//:protoc_lib'
    )
    native.bind(name='zlib', actual='@zlib_archive//:zlib')
    native.bind(name='grpc_lib', actual='@grpc//:grpc++')
