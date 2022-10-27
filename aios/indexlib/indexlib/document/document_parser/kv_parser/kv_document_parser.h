#ifndef __INDEXLIB_KV_DOCUMENT_PARSER_H
#define __INDEXLIB_KV_DOCUMENT_PARSER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/document/document_parser.h"

DECLARE_REFERENCE_CLASS(document, MultiRegionExtendDocFieldsConvertor);
DECLARE_REFERENCE_CLASS(document, MultiRegionPackAttributeAppender);
DECLARE_REFERENCE_CLASS(document, MultiRegionKVKeyExtractor);
DECLARE_REFERENCE_CLASS(config, IndexSchema);

IE_NAMESPACE_BEGIN(document);

class KvDocumentParser : public DocumentParser
{
public:
    KvDocumentParser(const config::IndexPartitionSchemaPtr& schema);
    ~KvDocumentParser();
public:
    bool DoInit() override;
    DocumentPtr Parse(const IndexlibExtendDocumentPtr& extendDoc) override;
    DocumentPtr Parse(const autil::ConstString& serializedData) override;

protected:
    virtual bool InitKeyExtractor();
    virtual void SetPrimaryKeyField(const IndexlibExtendDocumentPtr &document,
                                    const config::IndexSchemaPtr &indexSchema,
                                    regionid_t regionId, DocOperateType opType);

private:
    DocumentPtr CreateDocument(const IndexlibExtendDocumentPtr &document);
    
protected:
    MultiRegionExtendDocFieldsConvertorPtr mFieldConvertPtr;
    MultiRegionPackAttributeAppenderPtr mPackAttrAppender;
    
private:
    document::MultiRegionKVKeyExtractorPtr mKvKeyExtractor;
    
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(KvDocumentParser);

IE_NAMESPACE_END(document);

#endif //__INDEXLIB_KV_DOCUMENT_PARSER_H
