<a name="b08a3f9e"></a>
# function插件

<a name="e05dce83"></a>
## 简介

function插件主要对过滤、统计、排序以及distinct的功能做了进一步的扩展。用户自定义的插件可以完成以下功能：

- 可以通过query指定在过滤、统计、排序以及distinct中使用function插件
- 可以将任意attribute或者另外一个function作为参数，从而获取表达式的结果。
- function中声明的数据可以传递给scorer插件和qrs插件

例如：用户仅想展示具有某些属性的搜索结果，如果采用以前的filter子句需要写成filter=attribute=a OR attribute=b OR atrribute =c·· ....；此时，用户自定义一个函数表达式attribute_func，将这个attribute 作为参数，并传入一个值的列表，自己便可以根据列表决定是否保留每个doc。语法如下filter=attribute_func(attribute, "a|b|c")。

<a name="d412e121"></a>
## 主要处理流程

<a name="138c8b92"></a>
### QRS上主要处理流程

在Qrs上需要获取插件中包含的function信息，包括其能接收的参数数量、返回值类型、返回值是否为多值。获取到这些信息后用于Qrs上的语法验证，语法验证失败的query 在qrs上直接返回错误。获取function信息的步骤如下：

![](https://cdn.nlark.com/lark/0/2018/png/114751/1542945640091-65217fe4-cd9b-451f-9335-9f0b52c83bb6.png#alt=Qrs%20%E7%AB%AF%E5%88%9D%E5%A7%8B%E5%8C%96%E6%AD%A5%E9%AA%A4)

首先调用Factory的createFuncExpression完成Function的创建，创建出Function对象以后，调用其beginRequest接口完成插件的准备工作。调用evaluate计算值，并将之返回给上层使用。最后调用endRequest接口，完成资源释放等结束工作。最后调用Function自身的destroy接口，销毁对象自身。

<a name="513cb196"></a>
### Function插件与Scorer、Qrs Processor的交互

FunctionProvider提供两个声明自定义数据的接口：declareVariable，declareGlobalVariable 分别用于声明与每篇doc相关和每条query相关的数据。Scorer插件可以通过名字找到对应的数据，因此，function可以传递数据给Scorer插件。如果声明时，指定needSerizlize为true，则在Function中自定义的数据可以传递给QrsProcessor插件。在Qrs Processor插件中可以通过Hit的getVariableValue获取function定义的与每篇Doc相关的自定义数据，通过Result的getGlobalVarialbes接口获取function定义的与每次query相关的数据。

```
Note:
各个应用应该自己保证用于标志自定义数据的名字不冲突
```
