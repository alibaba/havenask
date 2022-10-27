#ifndef __INDEXLIB_DOCUMENT_PARSER_H
#define __INDEXLIB_DOCUMENT_PARSER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/document/document.h"
#include "indexlib/document/extend_document/indexlib_extend_document.h"

DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);

IE_NAMESPACE_BEGIN(document);

/* document parser */
class DocumentParser
{
public:
    DocumentParser(const config::IndexPartitionSchemaPtr& schema)
        : mSchema(schema)
    {}
    
    virtual ~DocumentParser() {}
    
public:
    virtual bool DoInit() = 0;
    virtual DocumentPtr Parse(const IndexlibExtendDocumentPtr& extendDoc) = 0;
    // dataStr: serialized data which from call document serialize interface
    virtual DocumentPtr Parse(const autil::ConstString& serializedData) = 0;

    virtual DocumentPtr TEST_ParseMultiMessage(const autil::ConstString& serializedData)
    {
        assert(false);
        return DocumentPtr();
    }

public:
    bool Init(const DocumentInitParamPtr& initParam)
    {
        mInitParam = initParam;
        return DoInit();
    }

    const DocumentInitParamPtr& GetInitParam() const { return mInitParam; }

protected:
    config::IndexPartitionSchemaPtr mSchema;
    DocumentInitParamPtr mInitParam;
    
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(DocumentParser);

IE_NAMESPACE_END(document);

#endif //__INDEXLIB_DOCUMENT_PARSER_H
