---
toc: content
order: 3
---
# Hape configuration

#### Overview
* In hape_conf, use hape_conf/default when the user does not specify
* hape_conf/default is divided into the following parts
   * global.conf: basic cluster configurations
   * swift: havenask MSMQ template
   * havenask: the havenask engine template
* You can create a custom HAPE configuration in the hape_conf file to manage multiple clusters. The HAPE configuration must be in the same format as that of HAPE_CONF /DEFAULT.

#### Variable rendering
* Because the havenask configuration has a large amount of dynamically generated content, the hape configuration supports default variable rendering.
* The variable format is ${xxx}. Default variables include the following:
   * user: the username used to log on to the PolarDB-X instance.
   * userHome: the home directory of the user.
   * hapeRoot: hape root directory
   * localHost: local IP address
   * defaultImage: the default image.
   * All global.conf variables are used for rendering. For example, ${common.binaryPath} indicates that the binaryPath variable in the globa section of global.conf is used to render

### global.conf description
#### common
* Function: global parameter
* Parameters
   * domainZkRoot: indicates the root zk path of a data center. domainZkRoot/serviceName will be the root zk path of a cluster. When domainZkRoot is not specified, hape will help pull up the test zookeeper path.
   * binaryPath: indicates where the container process finds the havenask binary package. Generally, the default value is /ha3_install.
   * dataStoreRoot: indicates the address of the index, configuration, and metadata files generated during the running of the HavenAsk cluster. Supports relative paths relative to the hapeRoot. It must be an hdfs path when in multi-machine mode
   * hadoophome: indicates the hadoop path. The address must be in /home/\<user\> (you can set a soft link), and there must be a corresponding hadoop on all machines. The default value exists /usr/local/hadoop/hadoop points to the default path in the container.
   * javaHome: indicates the java path. The address must be under /home/\<user\> (you can set a soft link), and the corresponding java must be available on all machines. The javaHome parameter must match the hadoopHome parameter. The default value exists /opt/taobao/java points to the default path in the container.


#### swift
* Function: Swift cluster parameters
* Parameters
   * swiftZkRoot: indicates the swift zk path. When not specified, hape will help pull up the test swift.
   * serviceName:swift service name
   * image: image address
   * adminCpu: the CPU usage of the admin, divided by 100 is equal to the number of cores
   * adminMem: the memory usage of the admin. Unit: MB.
   * adminIpList: the IP address of the admin server, separated by semicolons. if this parameter is not specified, the local ip address is used. Multiple admin will elect a leader
   * workerIpList: the list of worker machine IP addresses, separated by semicolons. When not specified, the local ip will be used.

#### havenask
* Function: online cluster parameters
* Parameters
   * serviceName: the name of the havenask service.
   * image: image address
   * adminCpu: the CPU usage of the admin, divided by 100 is equal to the number of cores
   * adminMem: the memory usage of the admin. Unit: MB.
   * adminIpList: the IP address of the admin server. When not specified, the local ip will be used.
   * qrsIpList: the list of IP addresses of the QRS servers. Separate the IP addresses with semicolons (). if this parameter is not specified, the local ip address is used. Multiple admin will elect a leader
   * searcherIpList: the list of server IP addresses of the searcher. Separate them with semicolons. When not specified, the local ip will be used.
   * adminHttpPort:havenask admin http the service port. If you do not specify this parameter, the value 45800 is used.

#### bs
* Function: creates cluster parameters for all tables.
* Parameters
   * serviceName: the name of the buildservice service.
   * image: image address
   * adminCpu: the CPU usage of the admin, divided by 100 is equal to the number of cores
   * adminMem: the memory usage of the admin. Unit: MB.
   * adminIpList: the IP address of the admin server, separated by semicolons. if this parameter is not specified, the local ip address is used. Multiple admin will elect a leader
   * workerIpList: the list of worker machine IP addresses, separated by semicolons. When not specified, the local ip will be used.



### Cluster template description
#### havenask
* hippo/qrs_hippo.json
   * Function: pulls the process template of havenask qrs.
   * Parameters
      * count: the number of service rows
      * minHealthCapacity: Minimum availability. Admin will ensure that minHealthCapacity/100*count of services are available.
      * resources: process container resources
      * args: startup parameter
      * envs: environment variable
* hippo/searcher_hippo.json
   * Function: pulls the process template of havenask searcher.
   * Parameters
      * count: the number of service rows
      * minHealthCapacity: Minimum availability. Admin will ensure that minHealthCapacity/100*count of services are available.
      * resources: process container resources
      * args: startup parameter
      * envs: environment variable
* direct_table
   * Function: the configuration template of the direct write table in the havenask cluster. The template includes [analyzer configuration](../config/analyzer.md), [data processing configuration](../config/data_table.json.md), and [index creation parameter configuration](../config/clusterconfig.md).
* offline_table
   * Function: a template for configuring all the tables in the havenask cluster. The template includes [analyzer configuration](../config/analyzer.md), [data processing configuration](../config/data_table.json.md), and [parameter configuration](../config/clusterconfig.md).
* biz_config
   * The configuration information used when the online cluster is started, including the [analyzer configuration](../config/analyzer.md). When you modify the analyzer, you must modify the analyzer in the table and the analyzer.json configuration file in the biz_config file.
#### swift
* config/swift_hippo.json
   * Function: pulls the process template of the swift broker.
   * Parameters
      * count: the number of service rows
      * role_resource: process container resources
      * args: startup parameter
      * envs: environment variable
* swift.conf
   * Function: Swift cluster parameters
