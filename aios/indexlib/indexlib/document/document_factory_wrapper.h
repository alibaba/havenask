#ifndef __INDEXLIB_DOCUMENT_FACTORY_WRAPPER_H
#define __INDEXLIB_DOCUMENT_FACTORY_WRAPPER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/document/document_factory.h"
#include "indexlib/plugin/module_factory_wrapper.h"

IE_NAMESPACE_BEGIN(document);

class DocumentFactoryWrapper;
DEFINE_SHARED_PTR(DocumentFactoryWrapper);

// attention: wrapper lifecycle should cover pluginManager
class DocumentFactoryWrapper : public plugin::ModuleFactoryWrapper<DocumentFactory>
{
public:
    DocumentFactoryWrapper(const config::IndexPartitionSchemaPtr& schema =
                           config::IndexPartitionSchemaPtr())
        : ModuleFactoryWrapper(DocumentFactory::DOCUMENT_FACTORY_NAME)
        , mSchema(schema)
    {}
    
    ~DocumentFactoryWrapper();
    
public:
    using plugin::ModuleFactoryWrapper<DocumentFactory>::Init;
    using plugin::ModuleFactoryWrapper<DocumentFactory>::GetPluginManager;
    using plugin::ModuleFactoryWrapper<DocumentFactory>::IsBuiltInFactory;

public:
    RawDocument* CreateRawDocument(
            const DocumentInitParamPtr& initParam =
            DocumentInitParamPtr())
    {
        if (IsBuiltInFactory())
        {
            return mBuiltInFactory->CreateRawDocument(mSchema, initParam);
        }
        assert(mPluginFactory);
        return mPluginFactory->CreateRawDocument(mSchema, initParam);
    }

    RawDocument* CreateMultiMessageRawDocument(
        const DocumentInitParamPtr& initParam = DocumentInitParamPtr())
    {
        if (IsBuiltInFactory())
        {
            // TODO: buildin table support multi message doc
            return nullptr;
        }
        assert(mPluginFactory);
        return mPluginFactory->CreateMultiMessageRawDocument(mSchema, initParam);
    }    

    DocumentParser* CreateDocumentParser(
            const DocumentInitParamPtr& initParam = DocumentInitParamPtr())
    {
        if (IsBuiltInFactory())
        {
            return mBuiltInFactory->CreateDocumentParser(
                    mSchema, initParam);
        }
        assert(mPluginFactory);
        return mPluginFactory->CreateDocumentParser(
                mSchema, initParam);
    }

    DocumentParser* CreateDocumentParser(
            const DocumentInitParam::KeyValueMap& kvMap)
    {
        DocumentInitParamPtr param(new DocumentInitParam(kvMap));
        if (IsBuiltInFactory())
        {
            return mBuiltInFactory->CreateDocumentParser(mSchema, param);
        }
        assert(mPluginFactory);
        return mPluginFactory->CreateDocumentParser(mSchema, param);
    }

    RawDocumentParser* CreateRawDocumentParser(
        const DocumentInitParam::KeyValueMap& kvMap)
    {
        DocumentInitParamPtr param(new DocumentInitParam(kvMap));
        if (IsBuiltInFactory())
        {
            return mBuiltInFactory->CreateRawDocumentParser(mSchema, param);
        }
        assert(mPluginFactory);
        return mPluginFactory->CreateRawDocumentParser(mSchema, param);
    }
    
public:
    static DocumentFactoryWrapperPtr CreateDocumentFactoryWrapper(
        const config::IndexPartitionSchemaPtr& schema,
        const std::string& identifier,
        const std::string& pluginPath);

    
protected:
    DocumentFactory* CreateBuiltInFactory() const override;
    
private:
    config::IndexPartitionSchemaPtr mSchema;

private:
    IE_LOG_DECLARE();
};

IE_NAMESPACE_END(document);

#endif //__INDEXLIB_DOCUMENT_FACTORY_WRAPPER_H
