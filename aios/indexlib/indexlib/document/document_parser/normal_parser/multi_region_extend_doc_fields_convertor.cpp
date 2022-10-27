#include "indexlib/document/document_parser/normal_parser/multi_region_extend_doc_fields_convertor.h"

using namespace std;
IE_NAMESPACE_USE(config);

IE_NAMESPACE_BEGIN(document);
IE_LOG_SETUP(document, MultiRegionExtendDocFieldsConvertor);

MultiRegionExtendDocFieldsConvertor::MultiRegionExtendDocFieldsConvertor(
        const IndexPartitionSchemaPtr &schema)
{
    for (regionid_t id = 0; id < (regionid_t)schema->GetRegionCount(); id++)
    {
        ExtendDocFieldsConvertorPtr exFieldsConvertor(new ExtendDocFieldsConvertor(schema, id));
        _innerExtendFieldsConvertors.push_back(exFieldsConvertor);
    }
    assert(_innerExtendFieldsConvertors.size() >= 1);
}

void MultiRegionExtendDocFieldsConvertor::convertIndexField(
        const IndexlibExtendDocumentPtr &document, const FieldConfigPtr &fieldConfig)
{
    _innerExtendFieldsConvertors[document->getRegionId()]->convertIndexField(
            document, fieldConfig);
}

void MultiRegionExtendDocFieldsConvertor::convertAttributeField(
        const IndexlibExtendDocumentPtr &document,
        const FieldConfigPtr &fieldConfig)
{
    _innerExtendFieldsConvertors[document->getRegionId()]->convertAttributeField(
            document, fieldConfig);
}

void MultiRegionExtendDocFieldsConvertor::convertSummaryField(
        const IndexlibExtendDocumentPtr &document,
        const FieldConfigPtr &fieldConfig)
{
    _innerExtendFieldsConvertors[document->getRegionId()]->convertSummaryField(
            document, fieldConfig);
}

IE_NAMESPACE_END(document);


