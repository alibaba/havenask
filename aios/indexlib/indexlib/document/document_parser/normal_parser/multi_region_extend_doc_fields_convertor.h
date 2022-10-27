#ifndef __INDEXLIB_DOCUMENT_MULTIREGIONEXTENDDOCFIELDSCONVERTOR_H
#define __INDEXLIB_DOCUMENT_MULTIREGIONEXTENDDOCFIELDSCONVERTOR_H

#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/document/document_parser/normal_parser/extend_doc_fields_convertor.h"

IE_NAMESPACE_BEGIN(document);

class MultiRegionExtendDocFieldsConvertor
{
public:
    MultiRegionExtendDocFieldsConvertor(
            const config::IndexPartitionSchemaPtr &schema);
    ~MultiRegionExtendDocFieldsConvertor() {}
private:
    MultiRegionExtendDocFieldsConvertor(const MultiRegionExtendDocFieldsConvertor &);
    MultiRegionExtendDocFieldsConvertor& operator=(const MultiRegionExtendDocFieldsConvertor &);
    
public:
    void convertIndexField(const IndexlibExtendDocumentPtr &document,
                           const config::FieldConfigPtr &fieldConfig);
    void convertAttributeField(const IndexlibExtendDocumentPtr &document,
                               const config::FieldConfigPtr &fieldConfig);
    void convertSummaryField(const IndexlibExtendDocumentPtr &document,
                             const config::FieldConfigPtr &fieldConfig); 

private:
    std::vector<ExtendDocFieldsConvertorPtr> _innerExtendFieldsConvertors;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(MultiRegionExtendDocFieldsConvertor);

IE_NAMESPACE_END(document);

#endif //__INDEXLIB_DOCUMENT_MULTIREGIONEXTENDDOCFIELDSCONVERTOR_H
