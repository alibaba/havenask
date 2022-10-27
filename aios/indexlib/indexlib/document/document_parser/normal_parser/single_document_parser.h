#ifndef __INDEXLIB_SINGLE_DOCUMENT_PARSER_H
#define __INDEXLIB_SINGLE_DOCUMENT_PARSER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/document/index_document/normal_document/normal_document.h"
#include "indexlib/document/extend_document/indexlib_extend_document.h"

DECLARE_REFERENCE_CLASS(document, MultiRegionExtendDocFieldsConvertor);
DECLARE_REFERENCE_CLASS(document, SectionAttributeAppender);
DECLARE_REFERENCE_CLASS(document, MultiRegionPackAttributeAppender);
DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);
DECLARE_REFERENCE_CLASS(config, IndexSchema);
DECLARE_REFERENCE_CLASS(util, AccumulativeCounter);

IE_NAMESPACE_BEGIN(document);

class SingleDocumentParser
{
public:
    SingleDocumentParser();
    ~SingleDocumentParser();
    
public:
    bool Init(const config::IndexPartitionSchemaPtr& schema,
              util::AccumulativeCounterPtr& attrConvertErrorCounter);
    NormalDocumentPtr Parse(const IndexlibExtendDocumentPtr& extendDoc);

private:
    void SetPrimaryKeyField(const IndexlibExtendDocumentPtr &document,
                            const config::IndexSchemaPtr &indexSchema, regionid_t regionId);
    
    void AddModifiedFields(const IndexlibExtendDocumentPtr &document,
                           const NormalDocumentPtr &indexDoc);

    NormalDocumentPtr CreateDocument(const IndexlibExtendDocumentPtr &document);

    bool Validate(const IndexlibExtendDocumentPtr &document);

private:
    config::IndexPartitionSchemaPtr mSchema;
    MultiRegionExtendDocFieldsConvertorPtr mFieldConvertPtr;
    SectionAttributeAppenderPtr mSectionAttrAppender;
    MultiRegionPackAttributeAppenderPtr mPackAttrAppender;
    bool mHasPrimaryKey;
    IE_NAMESPACE(util)::AccumulativeCounterPtr mAttributeConvertErrorCounter;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SingleDocumentParser);

IE_NAMESPACE_END(document);

#endif //__INDEXLIB_SINGLE_DOCUMENT_PARSER_H
