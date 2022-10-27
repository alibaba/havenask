#ifndef __INDEXLIB_BUILT_IN_DOCUMENT_FACTORY_H
#define __INDEXLIB_BUILT_IN_DOCUMENT_FACTORY_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/document/document_factory.h"
#include "indexlib/document/raw_document/default_raw_document.h"

IE_NAMESPACE_BEGIN(document);

class BuiltInDocumentFactory : public DocumentFactory
{
public:
    BuiltInDocumentFactory();
    ~BuiltInDocumentFactory();
    
protected:
    RawDocument* CreateRawDocument(
            const config::IndexPartitionSchemaPtr& schema) override;
    
    virtual DocumentParser* CreateDocumentParser(
            const config::IndexPartitionSchemaPtr& schema) override;

private:
    static const size_t MAX_KEY_SIZE;
    KeyMapManagerPtr mHashKeyMapManager;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(BuiltInDocumentFactory);

IE_NAMESPACE_END(document);

#endif //__INDEXLIB_BUILT_IN_DOCUMENT_FACTORY_H
