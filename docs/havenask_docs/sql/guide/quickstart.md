---
toc: content
order: 0
---

# 快速开始
havenask镜像托管在Docker Hub，如果Docker Hub访问不稳定，推荐阿里云镜像，镜像平台: amd64
* 运行镜像：registry.cn-hangzhou.aliyuncs.com/havenask/ha3_runtime:1.1.0
* 开发镜像：registry.cn-hangzhou.aliyuncs.com/havenask/ha3_dev:1.1.0

## 启动服务
Havenask引擎提供了可以直接运行的镜像，无需重新编译即可快速构建搜索服务。通过下面的一系列命令可以快速拉起一个单机版havenask服务。更详细的介绍见[hape介绍](../petool/intro.md)和[hape单机模式](../petool/localmode.md)

通过镜像启动引擎的步骤如下：
* 创建容器
其中CONTAINER_NAME为指定的容器名
```
cd ~
git clone git@github.com:alibaba/havenask.git -b 1.1.0
cd ~/havenask/docker/havenask
docker pull registry.cn-hangzhou.aliyuncs.com/havenask/ha3_runtime:1.1.0
./create_container.sh <CONTAINER_NAME> registry.cn-hangzhou.aliyuncs.com/havenask/ha3_runtime:1.1.0
```

* 登陆容器
```
./<CONTAINER_NAME>/sshme
```

* 启动havneask服务
```
/ha3_install/hape start havenask
```
* 创建表
```
/ha3_install/hape create table -t in0 -s /ha3_install/example/cases/normal/in0_schema.json -p 1
```

* 写入数据
```
/ha3_install/sql_query.py --query "insert into in0 (createtime,hits,id,title,subject)values(1,2,4,'测试', '测试')"
```
* 查询数据
```
/ha3_install/sql_query.py --query "select * from in0"
```

## 编译代码

### 编译环境
* 请确保编译的机器内存在15G以上，mac编译时需调整Docker容器资源上限（包括CPU、Memory、Swap等），具体路径：Docker Desktop->setting->Resources。
* 请确保cpu位8core以上，不然编译比较慢。

### 获取开发镜像

```
docker pull registry.cn-hangzhou.aliyuncs.com/havenask/ha3_dev:1.1.0
```
### 下载代码
```
cd ~
git clone git@github.com:alibaba/havenask.git -b 1.1.0
```

### 启动容器
```
cd ~/havenask/docker/havenask
./create_container.sh <CONTAINER_NAME> registry.cn-hangzhou.aliyuncs.com/havenask/ha3_dev:1.1.0
```
### 以ssh的方式登陆容器
```
./<CONTAINER_NAME>/sshme
```

### 执行编译指令
```
cd ~/havenask
./build.sh
```
### 单独编译子项目
```
## 编译发布tar包
bazel build //aios/tools/hape:hape_tar -c opt --copt -g --strip=always --config=havenask
## 编译havenask引擎
bazel build //aios/sql:ha_sql -c opt --copt -g --strip=always --config=havenask
```