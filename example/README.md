# Example
* 开始检索之前，需要手动构建索引，目前只支持全量构建。构建索引需要准备配置和原始数据。

## Example目录结构说明
* data 测试数据
* common 测试工具脚本目录
* cases 运行样例
* cases/<case> 具体某个样例名
  * config 配置文件
  * workdir 引擎工作目录
      * workdir/bs.log 索引构建日志
      * workdir/local_search_12000/qrs/logs/ha3.log 在线引擎qrs查询日志
      * workdir/local_search_12000/in0_0/logs/ha3.log 在线引擎searcher查询日志
  * setup.py 在运行测试之前的自动准备脚本

* 具体样例


| case  |  样例说明|
|---|---|
|  normal | 默认样例，执行简单sql查询  |
| vector | havenask向量索引样例 |
| custom_analyzer| 以开源c++版jieba分词器为基础的定制havenask分词插件样例 |
| custom_analyzer_big| 基于jieba分词器搜索5万条数据样例 |

请根据业务需求和实际数据格式对配置和数据进行修改。如何修改配置和数据格式的详解请参考 [havenask Wiki](https://github.com/alibaba/havenask/wiki)


## 使用脚本测试

* 构建全量索引。如果报错可以查看example/\<case\>/workdir/bs.log文件，索引产出在example/\<case\>/workdir/runtimedata目录
```
cd ~/havenask/example/
## <ha3_install>代表编译结果路径，如果在运行镜像为/ha3_install，在编译镜像为~/havenask/ha3_install
## <case>代表example下某个case的目录名，如normal
python build_demo_data.py <ha3_install> <case>
```

* 启动havenask并查询数据。\<qrsHttpPort\>为查询端口。如果查询错误可以根据qrs日志（workdir/local_search_12000/qrs/logs/ha3.log）和searcher日志（workdir/local_search_12000/in0_0/logs/ha3.log）进行排查
```
python start_demo_searcher.py <ha3_install> <case> <qrsHttpPort>
```
* 如果启动失败可以尝试以下操作：
    1. 删除工作目录example/\<case\>/workdir
    2. 如果开启了多个havenask容器，尝试全部关闭并重启一个新容器
    3. 重新利用脚本构建索引并查询

* 假设\<qrsHttpPort\>为12345，可以直接通过curl命令进行查询测试，也可以使用脚本进行测试，下面是一些测试query。其他查询语法请参考 [havenask Wiki](https://github.com/alibaba/havenask/wiki)

```
python curl_http.py 12345 "query=select count(*) from in0"

python curl_http.py 12345 "query=select id,hits from in0 where MATCHINDEX('title', '搜索词典')"

python curl_http.py 12345 "query=select title, subject from in0_summary_ where id=1 or id=2"
```


## 手动测试

* 创建workdir
```
mkdir ~/havenask/example/<case>/workdir
```

* 构建全量索引。使用时请替换上面的按照路径和配置的路径，建议在指定-c、-d、-w参数时使用绝对路径。具体的参数含义可以执行~/havenask/ha3_install/usr/local/bin/bs -h查看
```
<ha3_install>/usr/local/bin/bs startjob \
    -c ~/havenask/example/config/normal_config/offline_config/ \
    -n in0 -j local -m full  \ 
    -d ~/havenask/example/data/test.data \
    -w ~/havenask/example/<case>/workdir -i \
    ./runtimedata -p 1 --documentformat=ha3
```


* 启动havenask。执行时请将下面的目录地址替换为真实的地址。具体参数含义可以执行python <ha3_install>/usr/local/lib/python/site-packages/ha_tools/local_search_starter.py -h查看。启动成功后，会输出qrs的http和arpc端口
```
### 进入workdir目录，
cd ~/havenask/example/<case>/workdir

### 启动havenask

python <ha3_install>/local_search_starter.py \
        -i ./runtimedata/ \
        -c ~/havenask/example/normal_config/online_config/  \
        -p 30468,30480  \
        -b ~/havenask/ha3_install 
```


* 获取qrs http端口后（假设为12345），可以直接通过curl命令进行查询测试，也可以使用脚本进行测试，下面是一些测试query。其他查询语法请参考：[havenask Wiki](https://github.com/alibaba/havenask/wiki)

```
python curl_http.py 12345 "query=select count(*) from in0"

python curl_http.py 12345 "query=select id,hits from in0 where MATCHINDEX('title', '搜索词典')"

python curl_http.py 12345 "query=select title, subject from in0_summary_ where id=1 or id=2"
```
