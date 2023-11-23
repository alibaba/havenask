---
toc: content
order: 0
---
# Hape配置介绍

## 简介
* Hape的配置位于hape_conf，当用户未指定时使用hape_conf/default
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
* 用于可以在hape_conf下新建自己定制的hape配置用于管理多个集群，格式需要与hape_conf/default一致

## 变量渲染
* 由于havenask配置存在大量动态生成的内容，因此hape配置支持默认变量渲染
* 变量格式为${xxx}，默认变量包括以下：
    * user：用户名
    * userHome: 用户home目录
    * hapeRoot: hape根目录
    * localHost: 本机ip
    * defaultImage: 默认镜像
    * 所有global.conf的变量会用于渲染。例如${common.binaryPath}表示用global.conf中globa一节的binaryPath变量来渲染

## global.conf全局配置
### global.conf.common
* 作用：全局参数
* 参数说明
    * domainZkRoot：表示一个机房的根zk路径。domainZkRoot/serviceName将会是某类集群的根zk路径。当domainZkRoot不指定时hape会帮助拉起测试zookeeper路径
    * binaryPath：表示容器进程从哪里找到havenask binary包，一般默认为/ha3_install
    * dataStoreRoot: 表示havenask集群运行中产生的索引、配置、元信息文件地址。支持相对于hapeRoot的相对路径。多机模式下时必须为hdfs路径
    * hadoopHome：表示hadoop路径，地址必须在/home/\<user\>下（可以设置软链接），且必须在所有机器上都有对应hadoop。存在默认值/usr/local/hadoop/hadoop指向容器内默认路径
    * javaHome: 表示java路径，地址必须在/home/\<user\>下（可以设置软链接），且必须在所有机器上都有对应java。javaHome必须和hadoopHome匹配。存在默认值/opt/taobao/java指向容器内默认路径
    * processorMode: 默认情况单机模式下hape拉起单机多容器的集群。当额外设置该字段设为local的时候，hape拉起单机单容器多进程的集群，更方便调试和开发


### global.conf.swift
* 作用：swift集群参数
* 参数说明
    * swiftZkRoot：表示swift zk路径。当不指定时hape会帮助拉起测试swift
    * serviceName：swift服务名
    * image：镜像地址
    * adminCpu：admin cpu用量，除以100等于核数
    * adminMem：admin 内存用量，单位MB
    * adminIpList：admin机器ip，分号分隔。当不指定时会用本机ip。多台admin会选举一个leader
    * workerIpList：worker机器ip列表，分号分隔。当不指定时会用本机ip
    * extraArgs：添加该选项，可以向admin追加启动参数。多个参数用英文分号分割
    * extraEnvs: 添加该选项，可以向admin追加环境变量，多个变量用英文分号分隔

### global.conf.havenask
* 作用：在线集群参数
* 参数说明
    * serviceName：havenask服务名
    * image：镜像地址
    * adminCpu：admin cpu用量，除以100等于核数
    * adminMem：admin 内存用量，单位MB
    * adminIpList：admin机器ip。当不指定时会用本机ip
    * qrsIpList：qrs机器ip列表，分号分隔。当不指定时会用本机ip。多台admin会选举一个leader
    * searcherIpList：searcher机器ip列表，分号分隔。当不指定时会用本机ip
    * adminHttpPort：havenask admin http服务端口，不指定时使用值为45800
    * extraArgs：添加该选项，可以向admin追加启动参数。多个参数用英文分号分割
    * extraEnvs: 添加该选项，可以向admin追加环境变量，多个变量用英文分号分隔

### global.conf.bs
* 作用：全量表构建集群参数
* 参数说明
    * serviceName：buildservice服务名
    * image：镜像地址
    * adminCpu：admin cpu用量，除以100等于核数
    * adminMem：admin 内存用量，单位MB
    * adminIpList：admin机器ip，分号分隔。当不指定时会用本机ip。多台admin会选举一个leader
    * workerIpList：worker机器ip列表，分号分隔。当不指定时会用本机ip
    * extraArgs：添加该选项，可以向admin追加启动参数。多个参数用英文分号分割
    * extraEnvs: 添加该选项，可以向admin追加环境变量，多个变量用英文分号分隔



## havenask进程调度配置
* hippo/qrs_hippo.json
    * 作用：拉起havenask qrs的进程模板。
    * 参数说明
        * count: 服务行数
        * minHealthCapacity：最小可用度。admin会保证minHealthCapacity/100*count的台服务可用
        * resources：进程容器资源
        * args：启动参数
        * envs：环境变量
* hippo/searcher_hippo.json
    * 作用：拉起havenask searcher的进程模板。
    * 参数说明
        * count: 服务行数
        * minHealthCapacity：最小可用度。admin会保证minHealthCapacity/100*count的台服务可用
        * resources：进程容器资源
        * args：启动参数
        * envs：环境变量

## swift进程调度配置
* config/swift_hippo.json
    * 作用：拉起swift broker的进程模板。
    * 参数说明
        * count: 服务行数
        * role_resource：进程容器资源
        * args：启动参数
        * envs：环境变量
* swift.conf
     * 作用：swift集群参数
     * 参数说明:
       * data_root_path 用于存储swift数据的路径
