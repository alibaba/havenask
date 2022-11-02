# Example
开始检索之前，需要手动构建索引，目前只支持全量构建。构建索引需要准备配置和原始数据。

## Example目录结构说明
* havenask编译结果目录: <ha3_install>
  * 如果是运行镜像默认/ha3_install
  * 如果是编译镜像默认/home/\<USER\>/havenask/ha3_install
* 工作目录: example/workdir
* 配置目录: example/config
* 数据目录: example/data
* 测试脚本: example/script

请根据业务需求和实际数据格式对配置和数据进行修改。如何修改配置和数据格式的详解请参考问天引擎文档


## 使用脚本测试

* 构建全量索引。如果报错可以查看example/workdir/bs.log文件，索引产出在example/workdir/runtimedata目录
```
/home/<USER>/havenask/example/scripts/build_demo_data.py <ha3_install>
```

* 启动havenask并查询数据。\<qrsHttpPort\>为查询端口
```
/home/<USER>/havenask/example/scripts/start_demo_searcher.py <ha3_install> <qrsHttpPort>
```

* 假设\<qrsHttpPort\>为12345，可以直接通过curl命令进行查询测试，也可以使用脚本进行测试，下面是一些测试query。其他查询语法请参考：[Havenask Wiki](https://github.com/alibaba/havenask/wiki)

```
python curl_http.py 12345 "query=select count(*) from in0"

python curl_http.py 12345 "query=select id,hits from in0 where MATCHINDEX('title', '搜索词典')"

python curl_http.py 12345 "query=select title, subject from in0_summary_ where id=1 or id=2"
```


## 手动测试

* 创建workdir
```
mkdir /home/<USER>/havenask/example/workdir
```

* 构建全量索引。使用时请替换上面的按照路径和配置的路径，建议在指定-c、-d、-w参数时使用绝对路径。具体的参数含义可以执行/home/<USER>/havenask/ha3_install/usr/local/bin/bs -h查看
```
<ha3_install>/usr/local/bin/bs startjob \
    -c /home/<USER>/havenask/example/config/normal_config/offline_config/ \
    -n in0 -j local -m full  \ 
    -d /home/<USER>/havenask/example/data/test.data \
    -w /home/<USER>/havenask/example/workdir -i \
    ./runtimedata -p 1 --documentformat=ha3
```


* 启动havenask。执行时请将下面的目录地址替换为真实的地址。具体参数含义可以执行python <ha3_install>/usr/local/lib/python/site-packages/ha_tools/local_search_starter.py -h查看。启动成功后，会输出qrs的http和arpc端口
```
### 进入workdir目录，
cd /home/<USER>/havenask/example/workdir

### 启动havenask

python <ha3_install>/local_search_starter.py \
        -i ./runtimedata/ \
        -c /home/<USER>/havenask/example/normal_config/online_config/  \
        -p 30468,30480  \
        -b /home/<USER>/havenask/ha3_install 
```


* 获取qrs http端口后（假设为12345），可以直接通过curl命令进行查询测试，也可以使用脚本进行测试，下面是一些测试query。其他查询语法请参考：[Havenask Wiki](https://github.com/alibaba/havenask/wiki)

```
python curl_http.py 12345 "query=select count(*) from in0"

python curl_http.py 12345 "query=select id,hits from in0 where MATCHINDEX('title', '搜索词典')"

python curl_http.py 12345 "query=select title, subject from in0_summary_ where id=1 or id=2"
```
