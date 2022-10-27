#ifndef __INDEXLIB_DOCUMENT_FACTORY_H
#define __INDEXLIB_DOCUMENT_FACTORY_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/document/raw_document.h"
#include "indexlib/document/document_parser.h"
#include "indexlib/document/raw_document_parser.h"
#include "indexlib/plugin/ModuleFactory.h"

IE_NAMESPACE_BEGIN(document);

class DocumentFactory : public plugin::ModuleFactory
{
public:
    DocumentFactory();
    virtual ~DocumentFactory();

public:
    void destroy() override
    {
        delete this;
    }
    
public:
    /* create raw document */
    RawDocument* CreateRawDocument(
            const config::IndexPartitionSchemaPtr& schema,
            const DocumentInitParamPtr& initParam);

    /* create multi message raw document */
    RawDocument* CreateMultiMessageRawDocument(
            const config::IndexPartitionSchemaPtr& schema,
            const DocumentInitParamPtr& initParam);

    /* create document parser */
    DocumentParser* CreateDocumentParser(
            const config::IndexPartitionSchemaPtr& schema,
            const DocumentInitParamPtr& initParam);

    /* create raw document parser */
    virtual RawDocumentParser* CreateRawDocumentParser(
        const config::IndexPartitionSchemaPtr& schema,
        const DocumentInitParamPtr& initParam)
    {
        return nullptr;
    }
    
protected:
    virtual RawDocument* CreateRawDocument(
            const config::IndexPartitionSchemaPtr& schema) = 0;
    
    virtual DocumentParser* CreateDocumentParser(
            const config::IndexPartitionSchemaPtr& schema) = 0;

    virtual RawDocument* CreateMultiMessageRawDocument(
        const config::IndexPartitionSchemaPtr& schema)
    {
        return nullptr;
    }

public:
    static const std::string DOCUMENT_FACTORY_NAME;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(DocumentFactory);

// user must provider this two functions:
//     1. create${DOCUMENT_FACTORY_NAME}
//     2. destory${DOCUMENT_FACTORY_NAME}
extern "C" plugin::ModuleFactory* createDocumentFactory();
extern "C" void destroyDocumentFactory(plugin::ModuleFactory *factory);

IE_NAMESPACE_END(document);

#endif //__INDEXLIB_DOCUMENT_FACTORY_H
