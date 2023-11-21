---
toc: content
order: 0
---

# Hape standalone mode

* standalone mode refers to pulling up multiple containers of the havenask cluster on one machine, mainly used for testing
* If any of the following operations is abnormal, see [Hape FAQ and troubleshooting](./problem).


### I. Prepare the environment and configuration

#### Prepare an environment
* The latest image is registry.cn-hangzhou.aliyuncs.com/havenask/ha3_runtime:latest. Different versions have different images. You can obtain the image version by referring to the [release page](https://github.com/alibaba/havenask/releases). Make sure that Docker is used to pull the image of the corresponding version.
* Make sure that the machine can log in to itself without password ssh, and the following command opens up the password-free login itself:
```
ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa
ssh-copy-id -i ~/.ssh/id_rsa `hostname -i`
```

#### Modify the data import configuration
* By default, hape uses /ha3_install/hape_conf/default as the cluster configuration without modification.
* For more information, see [Configure Hape clusters](./config).


### 2. Create a havenask cluster

* Create and enter a hape container
```
./docker/havenask/create_container.sh <container_name> <image>
./<container_name>/sshme
```

* (Optional) Verify that no exceptions are found in the running environment of HAPE.
```
hape validate
```

* Create a havenask cluster. Containers of all subsequent processes are pulled up on the current machine.
```
hape start havenask
```

### III. Table management
* There are two types of tables: write-through tables and full tables. You can create one or both of them.
* Create a write-through table
   * -t indicates the table name.
   * -p indicates the number of shards, which must be 1 under a single server
   * -s indicates the schema path.
```
hape create table -t in0 -p 1 -s /ha3_install/example/cases/normal/in0_schema.json

```
* (Slow startup speed, optional) Create a full meter
   * -t indicates the table name.
   * -p indicates the number of shards, which must be 1 under a single server
   * -s indicates the schema path.
   * -f indicates the full file path, which supports files or directories.
```
hape create table -t in1 -p 1 -s /ha3_install/example/cases/normal/in0_schema.json  -f /ha3_install/example/data/test.data
```
* You must start the BS service for the full table and start multiple BS tasks. Generally, it takes 3 to 5 minutes to take effect. Therefore, you can run the following command to view the BS status. For more information, see [Hape Cluster Status](./status.md).
```
hape gs bs

### When all processes with full are finished and only inc process is left, execute the following command to find that the corresponding table status is READY
hape gs table -t <table_name>
```

* If you want to delete a table, you can do the following
```
hape delete table -t <table-name>
```

### 4. View the cluster status
* After the table is created, you can use the following command to check the cluster status and determine whether the cluster is ready for reading and writing. For more information, see [Hape Cluster Status](./status.md).
```
### In the returned result, the cluster is ready when the clusterStatus of sqlClusterInfo is set to READY.
hape gs havenask
```

### V. Read and write data
* Run the following command to write data to a table through direct write:
```
/ha3_install/sql_query.py --query "insert into in0 (createtime,hits,id,title,subject)values(1,2,4, 'test', 'test')"
```

* Query data in a Lindorm wide table
```
/ha3_install/sql_query.py --query "select * from in0"
```

* If you want to view the value of a field of the TEXT type, you must execute the following statement to query the [summary index](../indexes/summary.md) in a union manner:
```
/ha3_install/sql_query.py --query "select in0_summary_.id, in0_summary_.title, in0_summary_.subject from in0 inner join in0_summary_ on in0.id=in0_summary_.id"
```



* Swift is required for real-time data push for full table writing. Currently, you recommend use swift c++/java client. python is written only to test tools and is not recommend for production use. you need to call the following command. the meaning of the swift python write tool parameter is:
   * -- zk swift service zk address
   * -- The topic swift topic name, which is generally equal to the table name by default.
   * -- The number of count swift topic shards, which is generally equal to the number of table shards by default.
   * -- The schema address of the schema table.
   * -- file The havenask data file to be written.
```
### Check the serviceZk address of swift service. The default configuration is zfs://<localhost>:2181/havenask/havenask-swift-local
hape gs swift

### Write test messages
/ha3_install/swift_writer.py --zk <serviceZk> --topic in1 --count 1 --schema /ha3_install/example/cases/normal/in0_schema.json --file /ha3_install/example/data/rt.data
```

### 6. Cluster cleanup
```
### Clean up the HavenAsk cluster and delete the container and zk
hape delete havenask

### Clean up the Swift cluster and delete the container and zk
hape delete swift
```

### Seven. More
* For more information about cluster subcommands, see [Hape subcommands](./command.md).
* For more O&M scenarios, see [Hape O&M scenarios](./scene.md).
* For more examples based on the standalone mode, see hape/example/README.md
