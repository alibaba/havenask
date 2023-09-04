# Hape常见问题与排查

## hape工作原理
* hape做的所有操作都是基于havenask/swift/bs admin的
* admin基于hape给出可调度的机器列表和集群目标对于worker进行调整。hape不直接控制worker


## havenask集群进程信息
* 进程对应的ip地址与容器名可以使用hape gs命令查看
* havenask集群默认在/home/\<user\>下创建所有进程的工作目录。可以在某一台机器下使用 ls ~ | grep havenask | grep \<serviceName\>（serviceName代表集群配置里面global.conf设定的服务名）
* 日志：
    * 所有进程有基本日志：
        * 启动日志：\<workdir\>/stdout.log 与\<workdir\>/stdin.log
    * havenask admin的主要日志：
        * 集群进程调度日志 \<workdir\>/hippo.log
        * 集群目标日志 \<workdir\>/suez.log
    * havenask worker的主要日志: 
        * sql引擎工作日志 \<workdir\>/ha_sql/logs/sql.log
    * swift admin的主要日志：
        * 集群进程调度日志 \<workdir\>/logs/hippo.log
        * 集群目标日志 \<workdir\>/logs/swift/swift.log
    * swift broker的主要日志：
        * broker工作日志 \<workdir\>/logs/swift/swift.log
* 登录容器：
    ```
    find <workdir> -name sshme
    ```
    ```
    bash <path-to>/sshme
    ```

* 手动进程重启：
    * 先进入容器，ps -aux找到并杀死进程
    * 然后执行进程启动脚本
    ```
    find <workdir> -name process_starter.sh
    ```

    ```
    bash <path-to>/process_starter.sh & 
    ```


## 一般排查方法

### hape命令提示ERROR信息
* 通过hape debug日志排查
    * hape命令都有-v的参数，可以打开debug级别的日志看到hape具体执行的命令

* 通过hape子命令排查
    * hape validate子命令会对集群做基础的校验，可以借助这个命令确定一些简单问题
    * hape gs子命令可以透出集群和表的状态


###  havenask集群行为不符合预期
* hape gs子命令查看是否启动admin和worker容器
* 如果worker未能成功启动，可以查看admin的hippo.log日志，搜索docker相关命令是否执行成功
* 如果worker已经成功启动了，可以使用hape gs命令找到集群worker所在，查看worker日志中是否有明显ERROR信息
* havenask worker还可以查看zone_config是否符合预期


## 常见问题
* hape validate/hape debug日志/havenask admin日志显示容器和进程启动异常
    * 一般会打出对应的容器和进程命令，可以手动执行看一下原因

* 容器创建不成功，docker ps --all 显示容器退出
    * 在对应机器上执行sudo chmod 666 /var/run/docker.sock



* 创建swift topic超时
    * 一般是因为多机模式下的hadoopHome和javaHome设置不对造成的
    * 使用hape gs swift命令查看broker是否启动成功
    * 查看admin和broker日志是否有异常

* qrs未达目标行数/hape gs havenask命令显示部分进程异常
    * 可以检查一下对应日志是否是端口被占用
    * 可以使用hape restart container命令重启节点
* 向量索引不生效，插入后查询得到空数据
    * 参考向量索引例子的模板/ha3_install/example/cases/vector/vector_conf，其中data_tables和普通的配置有所不同