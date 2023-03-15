<a name="d622e45f"></a>
# summary插件示例

动态摘要插件可以根据配置信息和用户实现的的动态摘要插件的算法抽取出要显示给用户的摘要。下面我们举例说明动态摘要的书写流程。

<a name="409d553c"></a>
## 期望实现功能

在本例中我们期望实现可以对summary中数字进行高亮显示等功能。

<a name="a8706bc0"></a>
## 创建子类SummaryModuleFactorySample

其createSummary方法负责创建summary对象，h头文件和cpp文件如下：

```
// .h
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/summary/SummaryModuleFactory.h>
#include <summary_demo/HighlightNumberExtractor.h>

USE_HA3_NAMESPACE(summary);
namespace pluginplatform {
namespace summary_plugins {

class SummaryModuleFactorySample : public SummaryModuleFactory
{
public:
    SummaryModuleFactorySample();
    virtual ~SummaryModuleFactorySample();
public:
    virtual bool init(const KeyValueMap &parameters) {
        return true;
    }
    virtual void destroy() {
        delete this;
    }
    virtual SummaryExtractor* createSummaryExtractor(const char *extractorName, 
            const KeyValueMap &parameters, isearch::config::ResourceReader *resourceReader,
            isearch::config::HitSummarySchema *hitSummarySchema) 
    {
        if (extractorName == std::string("HighlightNumberExtractor")) {
            std::vector<std::string> attrNames;
            attrNames.push_back(std::string("price"));
            return new HighlightNumberExtractor(attrNames);
        }
        return NULL; // return NULL when error occurs
    }
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(SummaryModuleFactorySample);

}}


// cpp

#include <common/SummaryModuleFactorySample.h>

USE_HA3_NAMESPACE(util);
USE_HA3_NAMESPACE(summary);
namespace pluginplatform {
namespace summary_plugins {
HA3_LOG_SETUP(summary, SummaryModuleFactorySample);

SummaryModuleFactorySample::SummaryModuleFactorySample() { 
}

SummaryModuleFactorySample::~SummaryModuleFactorySample() { 
}

extern "C" 
ModuleFactory* createFactory() {
    return new SummaryModuleFactorySample;
}

extern "C" 
void destroyFactory(ModuleFactory *factory) {
    delete factory;
}

}}
```

接口说明：

- 接口：virtual SummaryExtractor* createSummaryExtractor(), 该接口用来创建summaryExtractor对象，在我们的例子中我们创建了HighlightNumberExtractor对象。

<a name="09c893f8"></a>
## 创建HighlightNumberExtractor子类

这个是继承自SummaryExtractor基类的动态摘要插件。h头文件和cpp文件如下：

```
//h
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/summary/SummaryExtractor.h>

USE_HA3_NAMESPACE(summary);
namespace pluginplatform {
namespace summary_plugins {

class HighlightNumberExtractor : public SummaryExtractor
{
public:
    HighlightNumberExtractor(const std::vector<std::string> &toSummaryAttributes);
    HighlightNumberExtractor(const HighlightNumberExtractor &);
    ~HighlightNumberExtractor();
private:
    HighlightNumberExtractor& operator=(const HighlightNumberExtractor &);
public:
    /* override */ void extractSummary(isearch::common::SummaryHit &summaryHit);
    /* override */ bool beginRequest(SummaryExtractorProvider *provider);
    /* override */ SummaryExtractor* clone();
    /* override */ void destory();
    /* override */ void endRequest() {}
private:
    void highlightNumber(const std::string &input, std::string &output, 
                         const isearch::config::FieldSummaryConfig *configPtr);
private:
    std::vector<std::string> _attrNames;
    const isearch::config::FieldSummaryConfigVec *_configVec;
    std::set<std::string> _keywords;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(HighlightNumberExtractor);

}}

//cpp
#include <summary_demo/HighlightNumberExtractor.h>

using namespace std;
using namespace autil;

USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(config);
USE_HA3_NAMESPACE(summary);
namespace pluginplatform {
namespace summary_plugins {
HA3_LOG_SETUP(summary_plugins, HighlightNumberExtractor);

HighlightNumberExtractor::HighlightNumberExtractor(
        const vector<string> &attrNames) 
    : _attrNames(attrNames)
    , _configVec(NULL)
{ 
}

HighlightNumberExtractor::HighlightNumberExtractor(
        const HighlightNumberExtractor &other)
{
    _attrNames = other._attrNames;
    _configVec = other._configVec;
    _keywords = other._keywords;
}

HighlightNumberExtractor::~HighlightNumberExtractor() { 
}

void HighlightNumberExtractor::extractSummary(isearch::common::SummaryHit &summaryHit) {
    FieldSummaryConfig defaultConfig;
    for (summaryfieldid_t fieldId = 0; 
         fieldId < (summaryfieldid_t)summaryHit.getFieldCount(); ++fieldId)
    {
        const ConstString *inputStr = summaryHit.getFieldValue(fieldId);
        if (!inputStr) {
            continue;
        }
        const FieldSummaryConfig *configPtr = NULL;
        if ((size_t)fieldId < _configVec->size()) {
            configPtr = (*_configVec)[fieldId]; 
        }
        if (configPtr == NULL) {
            configPtr = &defaultConfig;
        }
        string output;
        string input(inputStr->data(), inputStr->size());
        highlightNumber(input, output, configPtr);
        summaryHit.setFieldValue(fieldId, output.data(), output.size());
    }
}

void HighlightNumberExtractor::highlightNumber(
        const string &input, string &output, 
        const FieldSummaryConfig *configPtr)
{
    size_t beginPos = 0;
    bool foundNumber = false;
    for (size_t i = 0; i < input.size(); i++) {
        if ('0' <= input[i] && input[i] <= '9') {
            foundNumber = true;
            continue;
        }
        if (foundNumber) {
            output += configPtr->_highlightPrefix;
            for (size_t j = beginPos; j < i; j++) {
                output += input[j];
            }
            output += configPtr->_highlightSuffix;
        }
        output += input[i];
        foundNumber = false;
        beginPos = i+1;
    }
    if (foundNumber) {
        output += configPtr->_highlightPrefix;
        for (size_t j = beginPos; j < input.size(); j++) {
            output += input[j];
        }
        output += configPtr->_highlightSuffix;
    }
}

bool HighlightNumberExtractor::beginRequest(SummaryExtractorProvider *provider) {
    _configVec = provider->getFieldSummaryConfig();
    const SummaryQueryInfo *queryInfo = provider->getQueryInfo();
    const vector<isearch::common::Term> &terms = queryInfo->terms;
    _keywords.clear();
    for (size_t i = 0; i < terms.size(); i++) {
        _keywords.insert(string(terms[i].getWord().c_str()));
    }
    for (size_t i = 0; i < _attrNames.size(); i++) {
        summaryfieldid_t summaryFieldId = 
            provider->fillAttributeToSummary(_attrNames[i]);
        if (summaryFieldId == INVALID_SUMMARYFIELDID) {
            // this field is already in use
            return false;
        }
    }
    return true;
}
SummaryExtractor* HighlightNumberExtractor::clone() {
    return new HighlightNumberExtractor(*this);
}

void HighlightNumberExtractor::destory() {
    delete this;
}

}}
```

接口说明：

- 接口：bool beginRequest(SummaryExtractorProvider *provider)

   - 作用：在每一个request之前调用，可以进行一些初始化操作。
   - 参数说明：SummaryExtractorProvider，从查询信息中获得需要的数据。
- 接口：void extractSummary(common::SummaryHit &summaryHit);

   - 作用：通过它抽取动态摘要。
   - 参数说明：SummaryHit 最终结果，可以通过setFieldValue、getFieldValue以及clearFieldValue来进行操作。
- 接口：void endRequest()

   - 作用：做一些结束之前的收尾操作或者释放资源。