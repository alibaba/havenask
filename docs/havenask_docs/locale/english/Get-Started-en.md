Images in the following section are hosted on Docker Hub. If the connection to Docker Hub is unstable, you can try to download the images from Alibaba Cloud Container Registry.

* Runtime images: registry.cn-hangzhou.aliyuncs.com/havenask/ha3_runtime:0.2.2

* Development images: registry.cn-hangzhou.aliyuncs.com/havenask/ha3_dev:0.2.2



# Run the container

Havenask provides runtime images and allows you to quickly build a search service without recompiling workloads.



To start the engine by using an image, perform the following steps:

* Create a container.

   CONTAINER_NAME specifies the name of the container.

```

git clone git@github.com:alibaba/havenask.git

cd havenask/docker

docker pull havenask/ha3_runtime:0.2.2

./create_container.sh <CONTAINER_NAME> havenask/ha3_runtime:0.2.2

```



* Log on to the container.

```

./<CONTAINER_NAME>/sshme

```



# Test the container

After you start the runtime container, build the index of the test data and query engine. For more information, see [Example](https://github.com/alibaba/havenask/tree/main/example).



# Compile code



## Prepare the compiling environment

* Make sure that the machine used for compiling has more than 15 GB of memory. During compilation on MacOS, you must adjust the upper limit of Docker container resources such as the CPU, memory, and swap. You can choose Docker Desktop > Setting > Resources to go to the resource management page.

* To ensure the compilation speed, make sure the CPU has at least 8 cores.



## Obtain the development image



```

docker pull havenask/ha3_dev:0.2.2

```

## Download code

```

cd ~

git clone git@github.com:alibaba/havenask.git

```



## Run the container

```

cd ~/havenask/docker

## Run the following command in Linux:

./create_container.sh <DOCKER_NAME> havenask/ha3_dev:0.2.2

## Run the following command in MacOS:

./create_container_mac.sh <DOCKER_NAME> havenask/ha3_dev:0.2.2

```

## Use SSH to log on to the container

```

cd ~/havenask/docker/<DOCKER_NAME>

## Run the the following command to use SSH to log on to the container:

./sshme

```



## Run the compilation commands

Run the following commands. If the "fatal error: Killed signal terminated program cc1plus" error is reported, the memory of the compiler is insufficient. Reduce the number of threads for compilation, for example, run the ./build 5 command to build five threads.

```

./configure

./build

```

## Compile the subproject separately

indexlib is used in the following sample code.

```

cd ~/havenask/aios/indexlib

## Specify the number of threads for compilation after -j.

scons install -j30

## Perform the unit testing. Enter the specified directory:

scons . -u -j20

```
