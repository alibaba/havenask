## havenask镜像说明
* havenask镜像分为os_base、ha3_dev、ha3_runtime三种

### os_base
* os_base是基础镜像，基于Alibaba Cloud Linux制作，加入了必要的linux工具

### ha3_dev
* ha3_dev基于os_base制作，加入了Google Bazel编译工具。另外，由于havenask目前还存在部分待开源的依赖库未整理完成，因此作为extern加入该镜像中

### ha3_runtime
* ha3_runtime基于os_base制作，加入了havenask运行时依赖。如果用户改写了havenask代码，需要编译havenask_package_tar目标，拷贝到runtime目录下来重新制作运行时镜像
