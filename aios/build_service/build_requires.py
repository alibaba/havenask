import os

arpc_version = '= 13.3.*'
swift_version = '= 1.7.*'
fslib_version = '= 0.7.*'
fslib_hdfs_version = '= 0.8.*'
hippo_version = '= 0.26.*'
worker_framework_version = '= 0.14.*'
master_framework_version = '= 0.22.*'
#apsara_version = '= 0.14.0'
hdfs_version = '= 2.6.*'
gperftools_version = '= 2.6.*'
zlib_version = ' = 1.2.7'
lua_version = '= 5.1.*'
beeper_version = '= 0.1.*'

requires = [
    ('alog', '= 13.3.*'),
    ('autil', '= 0.12.*'),
    ('arpc',  arpc_version),
    ('fslib-framework', fslib_version),
    ('indexlib',  '= 3.7.*'),
    ('swift-client-minimal', swift_version),
    ('gperftools', gperftools_version),
    ('zlib', zlib_version),
    ('lua', lua_version),
    ('beeper-framework', beeper_version),
]

fslib_plugin_requires = [
    ('fslib_zookeeper_plugin', fslib_version),
    ('fslib_hdfs_plugin', fslib_hdfs_version),
#    ('fslib_pangu_plugin',  fslib_version),
]

worker_requires = [
    ('worker_framework', worker_framework_version),
    ('master_framework', master_framework_version),
    ('hippo-devel', hippo_version),
    ('t_search-external-boost-static', '= 1.63.*'),
    ('mysql-connector-c++', '= 8.0.14'),
 ] + fslib_plugin_requires

tools_requires = [
    ('arpc',  arpc_version),
    ('master_framework', master_framework_version),
    ('hippo-devel',  hippo_version),
    ('hippo-tools', hippo_version),
] + fslib_plugin_requires

apsara_package_requires = [
#    ('apsara_tools',  apsara_version),
]

build_requires = [
    # ('swift-devel', swift_version),
    # ('swift-tools', swift_version),
    # ('libhdfs-cdh-devel', hdfs_version),
    ('http_arpc-devel', '= 0.5.*'),
#    ('apsara-sdk', apsara_version),
    ('worker_framework-devel', worker_framework_version),
    ('master_framework-devel', master_framework_version),
    ('libiconv', '= 1.14'),
    ('hiredis-devel',  '= 0.13.*'),
]

require_generator = lambda requires: reduce(lambda x,y:x+y,
                                            map(lambda item:
                                                map(lambda x: '%s %s' % (item[0], x), item[1:]), requires))

def devel_filter(x):
    if x[0] == 'swift-client-minimal' or x[0] == 'apsara-protobuf' or x[0] == 'beeper-framework':
        return False
    return True

devel_transform = lambda x: (x[0] + '-devel',) + x[1:]
devel_require_generator = lambda x: require_generator(map(devel_transform, filter(devel_filter, x)))

rpm_requires = require_generator(requires)
worker_rpm_requires = require_generator(worker_requires)
sdk_rpm_requires = devel_require_generator(requires)
tools_rpm_requires = require_generator(tools_requires)
#apsara_package_rpm_requires = require_generator(apsara_package_requires)
apsara_package_rpm_requires = []

build_spec_template = '''
Name: build_service

%(build_requires)s

%%package sdk
%%package worker
%%package tools
%%package hadoop_package
#%%package apsara_package
'''

def genBuildSpec():
    build_specs = set(rpm_requires + worker_rpm_requires + sdk_rpm_requires +
                      tools_rpm_requires + apsara_package_rpm_requires)
    build_specs.update(set(require_generator(build_requires)))
    build_specs = sorted(list(build_specs))
    build_spec = os.getcwd() + '/_build.spec'
    f = open(build_spec, 'w')
    f.write(build_spec_template % { 'build_requires' :
                                    '\n'.join(map(lambda x: 'BuildRequires: %s' % x, build_specs)) })
    f.close()
