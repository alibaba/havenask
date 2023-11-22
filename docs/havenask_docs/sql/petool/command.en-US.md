---
toc: content
order: 1
---

# hape command

* Run the following command to get help for hape

```
hape -h
```

* Subcommands can also get help, such as

```
hape start -h

hape start havenask -h
```

## Command

### start
* Function: pulls a cluster
* Parameter description:
   * If you pass in the havenask parameter, the havenask cluster is pulled up.
   * Passing in swift means pulling up the swift cluster
* Valid values:
   * -c \<config\> is optional, indicating that the hape configuration is specified. If this parameter is not specified, the configuration is /ha3_install/hape_conf/default by default.
   * -v Optional. This indicates that the debug level log is enabled.
* Descriptions
   * When the parameter is set to havenask
      * If the swift cluster does not exist, hape will automatically pull up according to the configuration.
      * If domainZkRoot of global.conf is not specified in the specified cluster configuration, HAPE will automatically pull up the default test zookeeper.
```
### Pull up the Swift cluster
hape start swift

### Pull up the havenask cluster
hape start havenask

```


### stop
* Function: stops the cluster and does not clear the zookeeper information of the cluster.
* Parameter description:
   * havenask/swift/container indicates that the havenask cluster /swift cluster /a container is stopped.
* Valid values:
   * -c \<config\> is optional, indicating that the hape configuration is specified. If this parameter is not specified, the configuration is /ha3_install/hape_conf/default by default.
   * -v Optional. The value indicates that the debug-level log is enabled.
* Special options:
   * When the parameter is container (this information can be obtained from the gs subcommand)
      * -i \<ip\> ip address
      * -n \<name\> the name of the container to be stopped on the corresponding ip machine
```
### Stop the swift cluster
hape stop swift

### Stop the havenask cluster
hape stop havenask

### Stop a container on a machine
hape stop container -i <ip> -n <container-name>

```


### restart
* Function: restarts a cluster
* Parameter description:
   * havenask/swift/container indicates that the havenask cluster /swift cluster /a container is stopped.
   * container indicates to restart a container. For more information, see [Hape O&M](./scene.md).
* Valid values:
   * -c \<config\> is optional, indicating that the hape configuration is specified. If this parameter is not specified, the configuration is /ha3_install/hape_conf/default by default.
   * -v Optional. This indicates that the debug level log is enabled.
* Special options:
   * The following options are available when the parameter is container
      * -i \<ip\> Required. The IP address of the machine.
      * -n \<name\> Required. The name of the container.
```
### Restart the Swift cluster
hape restart swift

### Restart the havenask cluster
hape restart havenask

### Restart a container on a machine
hape restart container -i <ip> -n <container-name>
```

### delete
* Function: deletes a cluster or table and clears the zookeeper information of the cluster
* Parameter description:
   * havenask/swift/table/all indicates deleting a specific type of target
* Valid values:
   * -c \<config\> is optional, indicating that the hape configuration is specified. If this parameter is not specified, the configuration is /ha3_install/hape_conf/default by default.
   * -v Optional. This indicates that the debug level log is enabled.
* The following options are available when the parameter is table
   * -t \<table\> Required. This indicates that a specific table is deleted.
   * -- The keeptopic is optional. This parameter specifies whether to retain the swift topic when the table is deleted.

```
### Delete a Swift cluster
hape delete swift

### Delete the havenask cluster
hape delete havenask

### Delete the havenask table
hape delete table -t <table>

### Delete havenask, tables, and swift at a time
hape delete all

```

### create
* Function: creates a table
* Parameter description:
   * table indicates that a table is created.
* Valid values:
   * -c \<config\> is optional, indicating that the hape configuration is specified. If this parameter is not specified, the configuration is /ha3_install/hape_conf/default by default.
   * -v Optional. The value indicates that the debug-level log is enabled.
   * -t \<table\> Required. The name of the table.
   * -p \<partition-count\> Required. This parameter indicates the number of shards. The value must be 1 for a single server. In multi-machine mode, the number of shards must not exceed the number of searcher machines that can be used for scheduling. Otherwise, port conflicts may occur. In addition, in multi-machine mode, suppose there are N tables, of which the maximum shard is M, then the shard requirement of all tables must be either 1 or M
   * -s \<schema\> Required. This parameter indicates the schema path.
   * -f \<file\> is optional. It indicates the full file path. Files or directories are supported. If you specify this parameter, a full table is created. If you do not specify this parameter, a write-through table is created. You must specify the hdfs path in multi-machine mode
* Additional information:
   * A swift topic is automatically created when you create a table. You can also use swift tools to create a topic with the same name as the table in advance.
```
#### Write-through tables
hape create table -t in0 -p 1 -s /ha3_install/example/cases/normal/in0_schema.json

#### Full scale
hape create table -t in1 -p 1 -s /ha3_install/example/cases/normal/in0_schema.json  -f /ha3_install/example/cases/normal/test.data

```

### gs
* Function: obtains the status. For more information, see [Hape cluster status](./status.md).
* Options
   * swift/table/havenask/bs Get the status of swift/table/havenask/bs
* Parameter
   * -c \<config\> is optional, indicating that the hape configuration is specified. If this parameter is not specified, the configuration is /ha3_install/hape_conf/default by default.
   * -v Optional. The value indicates that the debug-level log is enabled.

* Special Options
   * The following options are available when the parameter is table
      * -t \<table-name\> Optional. This parameter is used to view only the status of a table.

```
#### Get the swift status
hape gs swift

#### Obtain the status of the havenask cluster.
hape gs havenask

#### Obtain the table status
hape gs table
hape gs table -t in0

#### Get the BS status
hape gs bs

```


### validate
* Function: performs basic checks on clusters, such as connectivity and whether container scheduling can be completed.
* Parameter
   * -c \<config\> is optional, indicating that the hape configuration is specified. If this parameter is not specified, the configuration is /ha3_install/hape_conf/default by default.
   * -v Optional. The value indicates that the debug-level log is enabled.

```
hape validate
```


### update
* Function: Updates some Hape configurations. For more information, see [Hape O&M scenarios](./scene.md).
* Parameter description:
   * candidate updates the list of IP addresses that can be used for scheduling in the cluster based on global.conf.
   * template Restart the havenask admin to load the latest template

* Parameter
   * -c \<config\> is optional, indicating that the hape configuration is specified. If this parameter is not specified, the configuration is /ha3_install/hape_conf/default by default.
   * -v Optional. The value indicates that the debug-level log is enabled.

```
### Update the IP address list of machines that can be used for scheduling
hape update candidate
### Restart havenask admin to load the latest template
hape update template
```

