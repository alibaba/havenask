# Document Processor 插件示例 #

## 插件功能 ##

可以实现同义词处理。比如用户输入“土豆”，那我们系统不仅要能返回包括“土豆”的文档，也还要返回包括“马铃薯”的文档。因此，我们在建索引的时候，就要对原始分词后的文档做一些处理，比如将原始文档中的“马铃薯”全部转换成“土豆”存储在系统中，那系统索引文档中就仅仅有“土豆”，而没有“马铃薯”了。但是，这个仅仅是一个方向的，我们还需要在 QRS 处写一个插件，将用户查询块中的“马铃薯”转换成“土豆”，然后利用“土豆”在系统保存的文档中查询，这样才能返回合适的结果。此外，还需要将要转换的数据（比如“马铃薯”到“土豆”）存储在配置文件里面.

## 插件编写 ##

首先，文档处理插件 RewriteTokenDocumentProcessor，功能是将原始文档中经过分词后的 TokenizerDocument 中的相关词（如“马铃薯”）替换成统一词（如“土豆”）。

我们要实现的第一个类是 RewriteTokenDocumentProcessor，必须继承自 DocumentProcessor 类。

```h
#include <ha3/common.h>
#include <build_service/processor/DocumentProcessor.h>
#include <build_service/config/ResourceReader.h>
#include <tr1/memory>

namespace pluginplatform {
namespace processor_plugins {

#define TOKEN_REWRITE_LIST "token_rewrite_list"
#define TOKEN_REWRITE_SEPARAROR ","
#define TOKEN_REWRITE_KEY_VALUE_SEPARATOR ":"

class RewriteTokenDocumentProcessor : public build_service::processor::DocumentProcessor
{
    typedef std::map<std::string, std::string> TokenRewriteMap;
public:
    RewriteTokenDocumentProcessor();
    ~RewriteTokenDocumentProcessor() {}
public:
    /* override */ bool process(const build_service::document::ExtendDocumentPtr& document);
    /* override */ void destroy();
    RewriteTokenDocumentProcessor* clone() {return new RewriteTokenDocumentProcessor(*this);}
    /* override */ bool init(const build_service::KeyValueMap &kvMap, 
                             const IE_NAMESPACE(config)::IndexPartitionSchemaPtr &schemaPtr, 
                             build_service::config::ResourceReader * resourceReader);
                         
    /* override */ std::string getDocumentProcessorName() const {return "RewriteTokenDocumentProcessor";}

private:
    IE_NAMESPACE(config)::IndexPartitionSchemaPtr _schemaPtr;
    TokenRewriteMap _tokenRewriteMap;
private:
    bool initTokenRewriteMap(const build_service::KeyValueMap &kvMap,
                             build_service::config::ResourceReader * resourceReader);
    bool constructTokenRewriteMap(const std::string &content);
private:
    BS_LOG_DECLARE();
};

typedef std::tr1::shared_ptr<RewriteTokenDocumentProcessor> RewriteTokenDocumentProcessorPtr;

}}

```

```cpp
#include "RewriteTokenDocumentProcessor.h"
#include <indexlib/document/index_document/normal_document/field_builder.h>
#include <build_service/processor/TokenizeDocumentProcessor.h>
#include <autil/StringTokenizer.h>
using namespace std;

IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(document);

using namespace build_service::document;

namespace pluginplatform {
namespace processor_plugins {

BS_LOG_SETUP(processor_plugins, RewriteTokenDocumentProcessor);

RewriteTokenDocumentProcessor::RewriteTokenDocumentProcessor()
{
}

bool RewriteTokenDocumentProcessor::init(const build_service::KeyValueMap &kvMap, 
        const IndexPartitionSchemaPtr &schemaPtr,
        build_service::config::ResourceReader *resourceReader)
{
    BS_LOG(DEBUG, "RewriteTokenDocumentProcessor::init");
    _schemaPtr = schemaPtr;
    return initTokenRewriteMap(kvMap,resourceReader);
}

bool RewriteTokenDocumentProcessor::process(const ExtendDocumentPtr& document)
{
    BS_LOG(DEBUG, "RewriteTokenDocumentProcessor::process");
    FieldSchemaPtr fieldSchema = _schemaPtr->GetFieldSchema();
    TokenizeDocumentPtr tokenizeDocument = document->getTokenizeDocument(); 
    assert(tokenizeDocument.get());
    assert(fieldSchema.get());

    for (FieldSchema::Iterator it = fieldSchema->Begin();
         it != fieldSchema->End(); ++it)
    {
        fieldid_t fieldId = (*it)->GetFieldId();
        TokenizeFieldPtr tokenizeFieldPtr = tokenizeDocument->getField(fieldId);
        if (!tokenizeFieldPtr.get()) {
            BS_LOG(WARN, "tokenizeFieldPtr == NULL");
            continue;
        }
        TokenizeField::Iterator sectionIt = tokenizeFieldPtr->createIterator();
        while(!sectionIt.isEnd()) {
            TokenizeSection::Iterator tokenIt = (*sectionIt)->createIterator();
            while(*tokenIt) {
                string word = string((*tokenIt)->getText().c_str());
                TokenRewriteMap::const_iterator rewriteIt = _tokenRewriteMap.find(word);
                if (rewriteIt != _tokenRewriteMap.end()) {
                    (*tokenIt)->setText(rewriteIt->second);
                    (*tokenIt)->setNormalizedText(rewriteIt->second);
                }
                tokenIt.next();
            }
            sectionIt.next();
        }
    }

    return true;
}

void RewriteTokenDocumentProcessor::destroy() 
{
    delete this;
}

bool RewriteTokenDocumentProcessor::initTokenRewriteMap(const build_service::KeyValueMap &kvMap, 
                             build_service::config::ResourceReader *resourceReader)
 {
     if (NULL == resourceReader) {
         return false;
     }
     string content;
     build_service::KeyValueMap::const_iterator it = kvMap.find(TOKEN_REWRITE_LIST);
     if (it != kvMap.end()) {
         if (resourceReader->getFileContent(content, it->second)) {
             return constructTokenRewriteMap(content);
         } else {
             BS_LOG(ERROR,"Can not read resource file:%s",it->second.c_str());
             return false;
         }
     } else {
         BS_LOG(ERROR,"Can not find the file!");
     }
    return true;
}

bool RewriteTokenDocumentProcessor::constructTokenRewriteMap(const std::string &content)
{
     autil::StringTokenizer stringTokenizer(content,
            TOKEN_REWRITE_SEPARAROR, 
            autil::StringTokenizer::TOKEN_IGNORE_EMPTY | autil::StringTokenizer::TOKEN_TRIM);
     for(size_t i=0; i<stringTokenizer.getNumTokens();i++)
     {
         autil::StringTokenizer strTokenizer(stringTokenizer[i],
            TOKEN_REWRITE_KEY_VALUE_SEPARATOR, 
            autil::StringTokenizer::TOKEN_IGNORE_EMPTY | autil::StringTokenizer::TOKEN_TRIM);
         if(strTokenizer.getNumTokens() != (size_t)2){
             BS_LOG(ERROR,"Can not parse key-value correctly!");
             return false;
         }
         _tokenRewriteMap.insert(make_pair(strTokenizer[0],
                         strTokenizer[1]));
     }
     return true;
}
}}

```

- 实现的类必须继承自 DocumentProcessor。
- 实现基类 DocumentProcessor 中的 process，destroy，clone，init，getDocumentProcessorName 方法。
- 一些初始化操作。其中，利用了 resourceReader 从资源文件中读取数据来完成初始化.
- 用配置文件（xxxx_cluster.json）里面的document processor 的parameters数据初始化tokenRewriteMap。
- 遍历 fieldSchema,获得 fieldId，然后在依据 fieldid 从 TokenizeDocumentPtr 中取出 TokenizeFieldPtr 对象，然后获取到 TokenizeSection，最后获取到 Token。
- 替换掉相应的词语。记住：一定要 setNormalizedText, 否则，修改的仅仅是一个副本，而不是原始数据。
- 利用 resourceReader 从资源文件中读取数据以初始化 _tokenRewriteMap


## 插件 Factory ##

第二个我们要实现的类是 RewriteTokenDocumentProcessorFactory，必须继承自 DocumentProcessorFactory 类，主要功能就是创建DocumentProcessor 类，具体代码如下：

```h
#include <ha3/common.h>
#include <tr1/memory>
#include <build_service/processor/DocumentProcessorFactory.h>
#include "processor_demo/RewriteTokenDocumentProcessor.h"

namespace pluginplatform {
namespace processor_plugins {

class RewriteTokenDocumentProcessorFactory : public build_service::processor::DocumentProcessorFactory
{
public:
    virtual ~RewriteTokenDocumentProcessorFactory() {}
public:
    virtual bool init(const build_service::KeyValueMap& parameters);
    virtual void destroy();
    virtual RewriteTokenDocumentProcessor* createDocumentProcessor(const std::string& processorName);
};

typedef std::tr1::shared_ptr<RewriteTokenDocumentProcessorFactory> RewriteTokenDocumentProcessorFactoryPtr;

}}

```

```cpp
#include "RewriteTokenDocumentProcessorFactory.h"

using namespace std;

namespace pluginplatform {
namespace processor_plugins {

bool RewriteTokenDocumentProcessorFactory::init(const build_service::KeyValueMap& parameters)
{
    return true;
}

void RewriteTokenDocumentProcessorFactory::destroy()
{
    delete this;
}
 
RewriteTokenDocumentProcessor* RewriteTokenDocumentProcessorFactory::createDocumentProcessor(
        const string& processorName)
{
    RewriteTokenDocumentProcessor* processor = new RewriteTokenDocumentProcessor;
    return processor;
}


extern "C"
build_service::util::ModuleFactory* createFactory() 
{
    return new RewriteTokenDocumentProcessorFactory;
}

extern "C"
void destroyFactory(build_service::util::ModuleFactory* factory) 
{
    factory->destroy();
}

}}
```

## 配置 ##

在 `data_table` 中配置 Document Processor 到对应的处理链上.

```
"processor_chain_config" : [
    {
        "document_processor_chain" : [
            {
                "class_name" : "TokenizeDocumentProcessor",
                "module_name": "",
                "parameters" : {}
            },
            {
                "class_name" : "RewriteTokenDocumentProcessor",
                "module_name": "rewriteToken",
                "parameters" : {
                    "token_rewrite_list" : "tokenRewriteFile.bin"
                }
            }
        ],
        "modules" : [
            {
                "module_name" : "rewriteToken",
                "module_path" : "libdocument_token_rewrite_processor.so",
                "parameters" : {
                }
            }
        ]
    }
]
```