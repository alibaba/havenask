---
toc: content
order: 0
---
# Hape configuration introduction

## Introduction
* The configuration of Hape is located in hape_conf, and hape_conf/default is used when not specified by the user
<Tree>
  <ul> 
    <li>default
      <ul>
        <ul>
        <li>global.conf (Hape全局配置)</li> 
        <li>cluster_templates
            <ul>
            <li>havenask (havenask引擎进程配置)
                <ul>
                    <li>...</li>
                </ul>
            </li>
            <li>swift (havenask消息队列进程配置)
                <ul><li>...</li></ul>
            </li>
            </ul>
        </li> 
        </ul>
    </li>
    </ul>
  </ul>
</Tree>
* Used to create customized hape configuration under hape_conf to manage multiple clusters. The format must be the same as that of hape_conf/default

## Variable rendering
* Due to the large amount of dynamically generated content in the havenask configuration, the hape configuration supports default variable rendering
* Variable format is ${xxx}, default variables include the following:
* user: indicates the user name
* userHome: indicates the user home directory
* hapeRoot: indicates the hape root directory
* localHost: indicates the local ip address
* defaultImage: default image
* All global.conf variables will be used for rendering. For example, ${common.binaryPath} represents the binaryPath variable in the globa section of glob.conf

conf Indicates the global configuration
### global.conf.common
* Function: global parameter
* Parameter Description
* domainZkRoot: indicates the root zk path of an equipment room. domainZkRoot/serviceName will be the root zk path of a cluster. hape helps pull up the test zookeeper path when domainZkRoot is not specified
* binaryPath: Where does the container process find the havenask binary package, usually by default /ha3_install
* dataStoreRoot: indicates the address of the index, configuration, and meta-information files generated during the running of the havenask cluster. Support for relative paths relative to hapeRoot. In multi-server mode, the directory must be hdfs
* hadoopHome: indicates the hadoop path. The address must be /home/\&lt. user\&gt; (Soft links can be set up), and the corresponding hadoop must be available on all machines. The default value /usr/local/hadoop/hadoop points to the default path in the container
* javaHome: indicates the java path, the address must be in /home/\&lt; user\&gt; (you can set soft links), and must have the corresponding java on all machines. javaHome must match hadoopHome. The default value /opt/taobao/java points to the default path in the container


### global.conf.swift
* Function: swift cluster parameter
* Parameter Description
* swiftZkRoot: indicates the swift zk path. When not specified, hape helps pull up the test swift
* serviceName: indicates the swift service name
* image: image address
* adminCpu: admin cpu usage, divided by 100 is the number of cores
* adminMem: admin Memory usage, unit MB
* adminIpList: admin machine ip, separated by semicolons. Local ip is used when not specified. Multiple admins elect a leader
* workerIpList: worker machine ip list, separated by semicolons. Local ip is used when not specified

### global.conf.havenask
* Function: Online cluster parameter
* Parameter Description
* serviceName: indicates the name of the havenask service
* image: image address
* adminCpu: admin cpu usage, divided by 100 is the number of cores
* adminMem: admin Memory usage, unit MB
* adminIpList: admin machine ip address. Local ip is used when not specified
* qrsIpList: qrs machine ip list, separated by semicolons. Local ip is used when not specified. Multiple admins elect a leader
* searcherIpList: searcher machine ip list, separated by semicolons. Local ip is used when not specified
* adminHttpPort: havenask admin http service port. If this parameter is not specified, the value is 45800

### global.conf.bs
* Function: The full scale builds cluster parameters
* Parameter Description
* serviceName: buildservice service name
* image: image address
* adminCpu: admin cpu usage, divided by 100 is the number of cores
* adminMem: admin Memory usage, unit MB
* adminIpList: admin machine ip, separated by semicolons. Local ip is used when not specified. Multiple admins elect a leader
* workerIpList: worker machine ip list, separated by semicolons. Local ip is used when not specified



## havenask Process scheduling configuration
* hippo/qrs_hippo.json
* Function: Pull up the process template of havenask qrs.
* Parameter Description
* count: indicates the number of service lines
* minHealthCapacity: Minimum availability. admin will ensure that the minHealthCapacity/100*count channel service is available
* resources: process container resources
* args: boot parameter
* envs: environment variable
* hippo/searcher_hippo.json
* Action: Pull up the process template for havenask searcher.
* Parameter Description
* count: indicates the number of service lines
* minHealthCapacity: Minimum availability. admin will ensure that the minHealthCapacity/100*count channel service is available
* resources: process container resources
* args: boot parameter
* envs: environment variable

## swift process scheduling configuration
* config/swift_hippo.json
* Role: Pull up swift broker's process template.
* Parameter Description
* count: indicates the number of service lines
* role_resource: process container resource
* args: boot parameter
* envs: environment variable
* swift.conf
* Function: swift cluster parameter
* Parameter Description:
* data_root_path Path for storing swift data