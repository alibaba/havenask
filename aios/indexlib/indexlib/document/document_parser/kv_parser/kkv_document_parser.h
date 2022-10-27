#ifndef __INDEXLIB_KKV_DOCUMENT_PARSER_H
#define __INDEXLIB_KKV_DOCUMENT_PARSER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/document/document_parser/kv_parser/kv_document_parser.h"

DECLARE_REFERENCE_CLASS(document, MultiRegionKKVKeysExtractor);

IE_NAMESPACE_BEGIN(document);

class KkvDocumentParser : public KvDocumentParser
{
public:
    KkvDocumentParser(const config::IndexPartitionSchemaPtr& schema);
    ~KkvDocumentParser();
    
public:
    bool InitKeyExtractor() override;
    void SetPrimaryKeyField(const IndexlibExtendDocumentPtr &document,
                            const config::IndexSchemaPtr &indexSchema,
                            regionid_t regionId, DocOperateType opType) override;

private:
    document::MultiRegionKKVKeysExtractorPtr mKkvKeyExtractor;
    
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(KkvDocumentParser);

IE_NAMESPACE_END(document);

#endif //__INDEXLIB_KKV_DOCUMENT_PARSER_H
