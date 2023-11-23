---
toc: content
order: 10
---

# 引擎开发

## 离线插件开发
* 离线数据处理的插件可以参考：aios/apps/facility/build_service/build_service/processor/BuildInDocProcessorFactory.cpp
* 离线分词器的插件开发可以参考：aios/plugins/havenask_plugins/analyzer_plugins

## 在线udf插件开发
* 参考：aios/plugins/havenask_plugins/udf_plugins。在线udf需要开发插件代码后重新build自己的runtime镜像
* 假设我们想实现一个UDF `sumOf` ，可以将输入的两个字段值相加并返回：

```sql
SELECT brand, sumOf(price, size) as sumOf FROM phone where brand='Xiaomi' ORDER BY price LIMIT 100
```

### 代码实现

需要实现的函数主要有以下两个：

- `beginRequest` ：Query开始时调用，可以在此处初始化一些变量供后续使用
- `evaluate` ：运行过程中，会将每条数据的相应字段送入该函数进行运算，并得到返回值作为最终结果
```cpp
template<typename T>
class SumOfFunction : public FunctionInterfaceTyped<T>
{
public:
    bool beginRequest(suez::turing::FunctionProvider *provider);
    T evaluate(matchdoc::MatchDoc matchDoc);
};
```


在这个需求中， `evaluate` 仅需要将字段值相加即可（注意，需要先对 `AttributeExpression` 执行 `evaluate` 之后，才可以使用 `getValue` 获取字段值）
```cpp
template<typename T>
class SumOfFunction : public FunctionInterfaceTyped<T>
{
// 此处有省略
public:
    bool beginRequest(suez::turing::FunctionProvider *provider) {
        return true;
    }
    T evaluate(matchdoc::MatchDoc matchDoc) {
        T value = T();
        for (size_t i = 0; i < _pAttrVec->size(); ++i) {
            (void)(*_pAttrVec)[i]->evaluate(matchDoc);
            value += (*_pAttrVec)[i]->getValue(matchDoc);
        }
        return value;
    }
private:
    std::vector<search::AttributeExpressionTyped<T>*> *_pAttrVec;
// 此处有省略
};
```

### 插件注册

完成UDF的代码实现后，该UDF还需要配置才可以使用。

#### 注册UDF
在aios/plugins/havenask_plugins/udf_plugins/HavenaskUdfFactory.cpp注册对应函数 
```cpp
bool HavenaskUdfFactory::registeFunctions() {
    REGISTE_FUNCTION_CREATOR(VectorScoreFunctionCreatorImpl);
    REGISTE_FUNCTION_CREATOR(BM25ScoreFunctionCreatorImpl);
    REGISTE_FUNCTION_CREATOR(TfIdfScoreFunctionCreatorImpl);
    // 添加到这里
    REGISTE_FUNCTION_CREATOR(SumOfFunctionCreatorImpl);
    return true;
}
```


#### 声明UDF原型

在aios/sql/misc/sql_function.json配置中注册UDF原型
```json
{
    "functions": [
        {
            "function_name": "sumOf",                      // 1
            "function_type": "UDF",                         // 2
            "is_deterministic": 1,                          // 3
            "function_content_version": "json_default_0.1", 
            "function_content": {
                "properties" : {},                          // 4
                "prototypes": [                             // 5
                    {
                        "params": [                         // 6
                            {
                                "is_multi": false,
                                "type": "double"
                            },
                            {
                                "is_multi": false,
                                "type": "double"
                            }
                        ],
                        "returns": [                        // 7
                            {
                                "is_multi": false,
                                "type": "boolean"
                            }
                        ]
                    }
                ]
            }
        }
    ]
}
```

说明：

1. 注册的插件名称
2. 注册的插件类型 `UDF`
3. is_deterministic：代表当前的函数在输入相同时输出结果是否确定。0代表不确定， 1代表确定。确定性的函数能支持更好的优化，比如下推执行；

     举例: random()函数每次调用会返回不同的值，其is_deterministic为0；

4. 用于注册插件的额外补充信息；
5. 插件注册支持注册多个函数原型，每一个原型是一个map结构；
6. 参数列表；
7. 返回值类型。


## 调试部署
### 调试方法
* 可以创建hape_dev镜像并登录容器，用仓库根目录的./build.sh脚本编译代码
* 一般来说，用户在开发havenask或者插件的时候都是用ha3_dev镜像编译开发的，不能直接拉起一个单机的ha3_runtime镜像测试最新的代码。为了解决这个问题，可以参考[hape配置](./config/hape-config.md)的global.conf.common中的processorMode字段，设值为local。此时执行hape命令相当于使用本地最新的代码，而非老版本runtime镜像里面的老代码

### 部署方法
* 用仓库根目录的docker/havenask/runtime/Dockerfile来build自己的runtime镜像
* 将/ha3_install/hape_conf中的global.conf的image替换为自己的runtime镜像

