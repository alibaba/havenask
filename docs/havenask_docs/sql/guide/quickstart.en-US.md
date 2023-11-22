---
toc: content
order: 0
---

# get-started-with-pipeline-of-new-version
The havenask image is hosted in Docker Hub. If the access to Docker Hub is unstable, recommend an Alibaba Cloud image. The image platform is amd64.
* Running Image: registry.cn-hangzhou.aliyuncs.com/havenask/ha3_runtime:latest
* Development image: registry.cn-hangzhou.aliyuncs.com/havenask/ha3_dev:latest

## enable the service
The Havenask engine provides images that can be run directly. You can quickly build a search service without recompiling. You can run the following commands to create a HAVENASK service of the unit price. To import full data or create a distributed cluster, see [HAVENASK](../petool/).

To start the engine by using an image, perform the following steps:
* Create containers
   CONTAINER_NAME is the name of the specified container.
```
cd ~
git clone git@github.com:alibaba/havenask.git
cd ~/havenask/docker/havenask
docker pull registry.cn-hangzhou.aliyuncs.com/havenask/ha3_runtime:latest
./create_container.sh <CONTAINER_NAME> registry.cn-hangzhou.aliyuncs.com/havenask/ha3_runtime:latest
```

* Log on to the container
```
./<CONTAINER_NAME>/sshme
```

* Start the havneask service
```
/ha3_install/hape start havenask
```
* Create a table
```
/ha3_install/hape create table -t in0 -s /ha3_install/hape_conf/example/cases/normal/in0_schema.json -p 1
```

* Write data to a table.
```
/ha3_install/sql_query.py --query "insert into in0 (createtime,hits,id,title,subject)values(1,2,4, 'test', 'test')"
```
* Query data
```
/ha3_install/sql_query.py --query "select * from in0"
```

## Run the following command to compile code in the RCETest.java file:

### Compilation environment
* Make sure that the host on which you want to compile data is larger than 15 GB. You must adjust the upper limit of Docker container resources (including CPU, memory, and Swap) during mac compilation. For more information, see Docker Desktop-> settings->Resources.
* Please ensure that the cpu bit is above 8core, otherwise the compilation is slow.

### Obtain a development image

```
docker pull registry.cn-hangzhou.aliyuncs.com/havenask/ha3_dev:latest
```
### Download code
```
cd ~
git clone git@github.com:alibaba/havenask.git
```

### Start containers
```
cd ~/havenask/docker/havenask
./create_container.sh <CONTAINER_NAME> registry.cn-hangzhou.aliyuncs.com/havenask/ha3_dev:latest
```
### Log on to the container by using ssh
```
./<CONTAINER_NAME>/sshme
```

### Execute compilation instructions
```
cd ~/havenask
./build.sh
```
### Compile subprojects individually
```
## Compile and publish the tar package
bazel build //package/havenask/hape:hape_tar --config=havenask
## Compile the havenask engine
bazel build //aios/sql:ha_sql --config=havenask
```
