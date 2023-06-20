# 一般排查方法
当遇到集群状态异常时，可以按照引擎启动的顺序排查
* domain daemon
    * 利用linux ps指令查看是否admin machine上正在运行domain_daemon.py（启动参数包含当前domain的名称）
* 容器是否运行
    * 利用gs子命令查看worker机器ip并登录，然后利用docker ps指令在worker机器上查看容器是否创建
* worker daemon是否运行
    * 利用gs子命令查看worker机器ip并登录，进入容器然后利用linux ps指令查看worker_daemon.py是否运行
* 目标是否下发
    * 在admin和worker的heartbeats目录下查看target是否符合预期
    * 如果target中包含源数据、索引、配置等路径，还需要看看这些文件是否已经下载到本地
* 引擎进程是否启动
    * qrs和searcher引擎可以登录容器后用ps指令查看sap_server_d是否运行，如果没有运行可以在worker的heartbeat文件中找到starter字段尝试手动运行查看报错
    * bs引擎可以登录容器后用linux ps指令查看startjob是否运行
* daemon和引擎日志是否有异常
    * 以上所有异常都可以结合daemon(admin&worker)和引擎日志进行排查
* 当一个role只有部分worker异常时，也可以尝试调用hape_cmd的start子命令进行重启
* hape_cmd还提供了check子命令帮助用户检查集群配置是否有基础性错误

# Bs常见异常
* 源数据、biz config(<worker工作目录>/worker_config)没有下发
* 索引没有产出(<worker工作目录>/runtimedata)

# Searcher常见异常
* 索引(<worker工作目录>/runtimedata)、biz config(<worker工作目录>/worker_config)没有下发
* part_searcher_starter脚本没有成功启动进程，可以查看worker daemon和引擎日志排查
* 重启searcher后无法查询，是因为需要重启qrs来更新searcher挂载信息

# Qrs常见异常
* biz config(<worker工作目录>/worker_config)没有下发
* searcher的subscribe_info没有下发到qrs或者有错误
* part_searcher_starter脚本没有成功启动进程，可以查看worker daemon和引擎日志排查
* 一个机器上部署了多个Qrs worker导致端口冲突


# docker常见异常
* 如果发现gs子命令一直返回preparing，且docker ps发现没有容器创建
    * 可以使用check子命令校验是否有基础错误
    * 可以执行docker ps -a查看是否有处于created状态的容器，如果有则进行手动清理
    * 用sudo chmod 666 /var/run/docker.sock命令修复