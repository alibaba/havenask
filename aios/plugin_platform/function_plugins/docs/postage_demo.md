<a name="8ea94bef"></a>
# function插件示例

function插件可以用于实现在语法中的一些表达式中加入用户自定义的一些函数，可以用于sort子句的表达式, filter子句的表达式, distinct子句中dist_key的表达式以及aggregate子句中group_key的表达式中。

<a name="409d553c"></a>
## 期望实现功能

比如用户可以自定义函数根据用户的地理位置以及卖家地理位置来动态计算邮费，我们希望实现一个语法中支持的函数（功能以及实现方法不一定具有实际意义），查询语句如下：

```
config=cluster:daogou&&query=test&&filter=price+postage(sellerCity, 311)<100
```

其中sellerCity是一个attribute，表示卖家所在城市的编码，311是买家所在城市编码，由前端获得。postage函数将会实现三个功能：

- 计算出精确邮费，用于计算商品实际价格（商品价格+邮费），筛选出小于100的document。
- 同时计算出买家和卖家之间的距离，送入相关性算分。
- 在filter阶段统计出最小的邮费（aggregate语法不能统计被filter过滤掉的document）。

<a name="6d8f12f0"></a>
## 实现PostageFunction类

首先我们需要实现function的逻辑功能，这个类主要是根据传入的每个doc的信息，计算出相应类型的结果。function的逻辑功能大都在其FunctionInterface类中实现，以PostageFunction为例，PostageFunction的返回值是邮费，所以返回值是double类型的，因此需要继承FunctionInterfaceTyped，同时evaluate的返回值也应该为double类型。除此之外，由于需要在声明数据传递给算分插件做后续处理，因此必须重载beginRequest函数。

```
//PostageFunction.h
#ifndef ISEARCH_POSTAGEFUNCTION_H
#define ISEARCH_POSTAGEFUNCTION_H

#include <autil/Log.h>
#include "turing_plugins/util/common.h"
#include "suez/turing/expression/function/FunctionCreator.h"
#include "suez/turing/expression/function/FunctionInterface.h"
#include "suez/turing/expression/function/FunctionMap.h"

BEGIN_TURING_PLUGINS_NAMESPACE(func_expression);

class PostageFunction : public suez::turing::FunctionInterfaceTyped<double>                               (1)
{
public:
    typedef std::map<std::pair<int32_t, int32_t>, std::pair<double, double> > PostageMap;
public:
    PostageFunction(suez::turing::AttributeExpressionTyped<int32_t> *sellerCityAttrExpr,
                  int32_t buyerCity, const PostageMap &postageMap);
    ~PostageFunction();
private:
    PostageFunction(const PostageFunction &);
    PostageFunction& operator=(const PostageFunction &);
public:
    /* override */ bool beginRequest(suez::turing::FunctionProvider *provider);                           (2)
    /* override */ double evaluate(matchdoc::MatchDoc matchDoc);                             (3)
    /* override */ void endRequest() {}                                                     (4)
private:
    suez::turing::AttributeExpressionTyped<int32_t> *_sellerCityAttrExpr;
    int32_t _buyerCity;
    const PostageMap &_postageMap;

    matchdoc::Reference<double> *_distanceRef;
    double *_minPostage;
private:
    AUTIL_LOG_DECLARE();
};

//////////////////////////////////////////////////////////////////////////////

DECLARE_FUNCTION_CREATOR(PostageFunction, "postage", 2);                                    (5)
class PostageFunctionCreatorImpl : public FUNCTION_CREATOR_CLASS_NAME(PostageFunction)      (6)
{
public:
    typedef PostageFunction::PostageMap PostageMap;
public:
    /* override */ bool init(const KeyValueMap &funcParameter,                              (7)
                             const suez::ResourceReaderPtr &resourceReader);

    /* override */ suez::turing::FunctionInterface *createFunction(
                            const suez::turing::FunctionSubExprVec& funcSubExprVec);                      (8)
private:
    PostageMap _postageMap;
};

END_TURING_PLUGINS_NAMESPACE(func_expression);

#endif //ISEARCH_POSTAGEFUNCTION_H


//PostageFunction.cpp
#include "PostageFunction.h"
#include <suez/turing/expression/framework/ArgumentAttributeExpression.h>

using namespace std;
using namespace suez::turing;

BEGIN_TURING_PLUGINS_NAMESPACE(func_expression);
AUTIL_LOG_SETUP(func_expression, PostageFunction);

bool PostageFunctionCreatorImpl::init(const KeyValueMap &funcParameter,
                                      const suez::ResourceReaderPtr &resourceReader)
{
    string fileContext;
    if (!resourceReader->getFileContent(fileContext, "postage_map")) {                      (9)
        return false;
    }
    istringstream iss(fileContext);
    pair<int32_t, int32_t> post;
    double price;
    double distance;
    while (iss >> post.first >> post.second >> price >> distance) {
        _postageMap[post] = make_pair(price, distance);
    }
    return true;
}

FunctionInterface *PostageFunctionCreatorImpl::createFunction(
                                      const FunctionSubExprVec& funcSubExprVec)
{
    if (funcSubExprVec.size() != 2) {                                                       (10)
        return NULL;
    }
    AttributeExpressionTyped<int32_t> *sellerCityAttrExpr =
        dynamic_cast<AttributeExpressionTyped<int32_t> *>(funcSubExprVec[0]);
    if (!sellerCityAttrExpr) {
        return NULL;
    }
    ArgumentAttributeExpression *arg =
        dynamic_cast<ArgumentAttributeExpression*>(funcSubExprVec[1]);
    if (!arg) {
        return NULL;
    }

    int32_t buyerCity;
    if (!arg->getConstValue<int32_t>(buyerCity)) {
        return NULL;
    }

    return new PostageFunction(sellerCityAttrExpr, buyerCity, _postageMap);
}

/////////////////////////////////////////////////////////////////////////

PostageFunction::PostageFunction(AttributeExpressionTyped<int32_t> *sellerCityAttrExpr,
                                 int32_t buyerCity, const PostageMap &postageMap)
    : _sellerCityAttrExpr(sellerCityAttrExpr)
    , _buyerCity(buyerCity)
    , _postageMap(postageMap)
    , _distanceRef(NULL)
    , _minPostage(NULL)
{
}

PostageFunction::~PostageFunction() {
}

bool PostageFunction::beginRequest(FunctionProvider *provider) {
    _distanceRef = provider->declareVariable<double>("distance", true);                     (11)
    if (!_distanceRef) {
        return false;
    }
    _minPostage = provider->declareVariable<double>("min_postage", true);             (12)
    if (!_minPostage) {
        return false;
    }
    *_minPostage = numeric_limits<double>::max();
    return true;
}

double PostageFunction::evaluate(MatchDoc matchDoc) {
    (void)_sellerCityAttrExpr->evaluate(matchDoc);                                          (13)
    int32_t sellerCity = _sellerCityAttrExpr->getValue(matchDoc);
    pair<int32_t, int32_t> post(sellerCity, _buyerCity);
    PostageMap::const_iterator it = _postageMap.find(post);
    double postage = 20.0; // default
    double distance = 10000.0; //default
    if (it != _postageMap.end()) {
        postage = it->second.first;
        distance = it->second.second;
    }
    if (*_minPostage > postage) {                                                           (14)
        *_minPostage = postage;
    }
    _distanceRef->set(matchDoc->getVariableSlab(), distance);                               (15)
    return postage;
}

END_TURING_PLUGINS_NAMESPACE(func_expression);
```

说明：

- 
1.由于返回值是double类型，所以必须继承FunctionInterfaceTyped。如果返回值和第一个参数一致，可以继承FunctionInterfaceTyped，在creator中根据第一个参数的类型来创建对应类型的function。

- 
2.每一次request到来前，在所有表达式创建完成之后，调用算分插件的beginRequest之前被调用一次，该函数可以通过FunctionProvider声明一些值，用来传递给算分插件或者QRS插件。

- 
3.对于每个match到的document根据MatchDoc中的信息以及自身的信息来计算得到相应结果，并返回。

- 
4.在结束request时会被调用，本例中不需要使用，可以不重载，但是如果有某些求平均值等需求希望在最后才计算的，可以在该函数中实现。

- 
5.声明PostageFunction对应的Creator的类，不同类型的函数有三种声明方式，这里我们应该用第一种声明方式，给定Function的类名，同时还需写入另外两个参数，一个是函数名，一个是该函数接受的参数个数，如果函数不限制参数个数（类似于sum这种函数）那么可以设置为config::FUNCTION_UNLIMITED_ARGUMENT_COUNT。
```
  - DECLARE_FUNCTION_CREATOR(classname, funcName, argCount)
  用于返回类型确定，函数的类不是模板类

  - DECLARE_TEMPLATE_FUNCTION_CREATOR(classname, return_type, funcName, argCount)
  用于返回类型确定，但是函数本身是模板类
  比如notin这种函数，返回值是bool，但是支持的类型可以是任意数字类型

  - DECLARE_UNKNOWN_TYPE_FUNCTION_CREATOR(classname, funcName, argCount)
  返回类型根据第一个参数而定，这时函数的类本身肯定是模板类
  例如add，返回值可以是任意数字类型，支持的参数也是任意数字类型
```


- 
6.真正传递给factory的创建PostageFunction类，必须继承自之前宏所声明的Creator类。

- 
7.creator的init函数在基类中有默认实现，如果不需要做初始化处理，则不必继承。该函数两个参数分别为该function在配置中配置的key/value值，以及可以用于读取config目录下文件的ResourceReader

- 
8.创建PostageFunction的函数，输入参数是PostageFunction的参数，Creator类必须实现该函数。

- 
9.Creator类init时，我们从config中读取postage_map文件，然后按照一定格式读入文件，因为是主要介绍function插件的例子，代码中对输入格式不对等问题没有做处理。

- 
10.创建PostageFunction对象，在creator中应该先对传入参数的合法性进行检查，如果参数不合法，那么应该返回NULL。这里我们检查了参数个数必须为2，第一个值必须是int32_t类型的，同时第二个参数是常数。

- 
11.创建出来的Function在request到来时可以声明变量，声明时要注意判断返回值，declareVariable得到每个MatchDoc都有的变量，填写值的过程后续会介绍，这里介绍一下两个参数的含义，第一个参数是声明的变量的名字，算分插件，QRS插件以及其他function插件可以通过这个名字来找到这个变量，从而得到相应的值；第二个参数含义是是否序列化传输到QRS上，只有选择序列化QRS插件才能得到这个值。

- 
12.同上11。

- 
13.把matchDoc传递给需要计算的AttributeExpression，并通过getValue来得到相应的结果。

- 
14.这里我们计算最小邮费，对每个得到的结果都对最小邮费进行修正。

- 
15.根据map中找到的卖家和买家的距离，记录到每个matchDoc中，便于算分插件根据这个距离来进行一定的处理。


<a name="fbe6c8f0"></a>
## 实现PostageFunctionFactory类

下面我们要实现的类是PostageFunctionFactory，这个类是插件的入口，在插件so被打开后首先会调用init函数，之后会被调用registeFunctions函数，这个函数主要是根据各个用户自定义的函数的Creator来注册到factory里，引擎会获取注册到的信息，并根据函数名来调用相应的Creator创建对应的函数，PostageFunctionFactory继承自SyntaxExpressionFactory类，同时还需要在cpp代码中实现createFactory函数以及destoryFactory函数，具体代码如下：

```
//PostageFunctionFactory.h
#ifndef ISEARCH_POSTAGEFUNCTIONFACTORY_H
#define ISEARCH_POSTAGEFUNCTIONFACTORY_H

#include "autil/Log.h"
#include <suez/turing/expression/function/SyntaxExpressionFactory.h>

BEGIN_TURING_PLUGINS_NAMESPACE(func_expression);

class PostageFunctionFactory : public suez::turing::SyntaxExpressionFactory
{
public:
    PostageFunctionFactory();
    ~PostageFunctionFactory();
private:
    PostageFunctionFactory(const PostageFunctionFactory &);
    PostageFunctionFactory& operator=(const PostageFunctionFactory &);
public:
    /* override */ bool init(const KeyValueMap &parameters);                        (1)
    /* override */ bool registeFunctions();                                         (2)
private:
    AUTIL_LOG_DECLARE();
};

TURING_PLUGINS_TYPEDEF_PTR(PostageFunctionFactory);

END_TURING_PLUGINS_NAMESPACE(func_expression);

#endif //ISEARCH_POSTAGEFUNCTIONFACTORY_H

//PostageFunctionFactory.cpp
#include "PostageFunctionFactory.h"
#include <functions/PostageFunction.h>

BEGIN_TURING_PLUGINS_NAMESPACE(func_expression);
AUTIL_LOG_SETUP(functions, PostageFunctionFactory);

PostageFunctionFactory::PostageFunctionFactory() {
}

PostageFunctionFactory::~PostageFunctionFactory() {
}

bool PostageFunctionFactory::init(const KeyValueMap &parameters) {
    return true;
}

bool PostageFunctionFactory::registeFunctions() {
    REGISTE_FUNCTION_CREATOR(PostageFunctionCreatorImpl);                           (3)
//    REGISTE_FUNCTION_CREATOR(OtherCreatorImpl);
    return true;
}

extern "C"
ModuleFactory* createFactory() {
    return new PostageFunctionFactory;                                              (4)
}

extern "C"
void destroyFactory(util::ModuleFactory *factory) {
    factory->destroy();
}
END_TURING_PLUGINS_NAMESPACE(func_expression);
```

说明：

- 1.FunctionFactory的init函数，在插件被load的时候（upc/loadPartition时）调用一次，传入参数为用户在配置中写入的参数，基类中有默认的空实现。
- 2.registeFunctions函数必须继承，该函数用于注册本插件所有的FunctionCreator。
- 3.REGISTE_FUNCTION_CREATOR用于注册creator，本例中只有一个creator，多个的写法即后面的OtherCreatorImpl（取消掉注释）。
- 4.插件接口，createFactory时new出自己的factory。
