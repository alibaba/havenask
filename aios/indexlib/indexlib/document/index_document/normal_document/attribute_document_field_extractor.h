#ifndef __INDEXLIB_ATTRIBUTE_DOCUMENT_FIELD_EXTRACTOR_H
#define __INDEXLIB_ATTRIBUTE_DOCUMENT_FIELD_EXTRACTOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include <autil/mem_pool/Pool.h>

DECLARE_REFERENCE_CLASS(config, AttributeSchema);
DECLARE_REFERENCE_CLASS(document, AttributeDocument);
DECLARE_REFERENCE_CLASS(common, PackAttributeFormatter);

IE_NAMESPACE_BEGIN(document);

class AttributeDocumentFieldExtractor
{
public:
    AttributeDocumentFieldExtractor();
    ~AttributeDocumentFieldExtractor();
public:
    bool Init(const config::AttributeSchemaPtr& attributeSchema);
    autil::ConstString GetField(
            const document::AttributeDocumentPtr& attrDoc,
            fieldid_t fid, autil::mem_pool::Pool *pool); 

private:
    std::vector<common::PackAttributeFormatterPtr> mPackFormatters;
    config::AttributeSchemaPtr mAttrSchema;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(AttributeDocumentFieldExtractor);

IE_NAMESPACE_END(document);

#endif //__INDEXLIB_ATTRIBUTE_DOCUMENT_FIELD_EXTRACTOR_H
