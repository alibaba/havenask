#include "SimpleDocumentProcessor.h"
#include "build_service/analyzer/Token.h"
#include <indexlib/document/index_document/normal_document/field_builder.h>

using namespace std;
using namespace autil::legacy;
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(document);
using namespace build_service::document;
namespace build_service {
namespace processor {

SimpleDocumentProcessor::SimpleDocumentProcessor() {
}

SimpleDocumentProcessor::~SimpleDocumentProcessor() {
}

bool SimpleDocumentProcessor::init(const KeyValueMap &kvMap,
                                   const IndexPartitionSchemaPtr &schemaPtr,
                                   config::ResourceReader *resourceReader)
{
    if (kvMap.find("initFailFlag") != kvMap.end()
        && kvMap.find("initFailFlag")->second == "true")
    {
        return false;
    }
    _schemaPtr = schemaPtr;
    return true;
}

bool SimpleDocumentProcessor::process(const ExtendDocumentPtr& document)
{
    FieldSchemaPtr fieldSchema = _schemaPtr->GetFieldSchema();
    IndexSchemaPtr indexSchema = _schemaPtr->GetIndexSchema();
    AttributeSchemaPtr attrSchema = _schemaPtr->GetAttributeSchema();
    SummarySchemaPtr summarySchema = _schemaPtr->GetSummarySchema();

    RawDocumentPtr rawDocument = document->getRawDocument();
    TokenizeDocumentPtr tokenizeDocument = document->getTokenizeDocument();

    for (FieldSchema::Iterator it = fieldSchema->Begin();
         it != fieldSchema->End(); ++it)
    {
        fieldid_t fieldId = (*it)->GetFieldId();
        FieldType fieldType = (*it)->GetFieldType();
        if (indexSchema->IsInIndex(fieldId) || (fieldType == ft_text)) {
            TokenizeFieldPtr field = tokenizeDocument->createField(fieldId);
            if (field.get() == NULL) {
                //LOG(ERROR, "create TokenizeField failed: [%d]", fieldId);
                return false;
            }
            TokenizeSection *section = field->getNewSection();
            TokenizeSection::Iterator iterator = section->createIterator();
            analyzer::Token token;
            const string& fieldName = (*it)->GetFieldName();
            const string& fieldValue = rawDocument->getField(fieldName);

            token.setText(fieldValue);
            token.setNormalizedText(fieldValue.c_str());
            section->insertBasicToken(token, iterator);
        }

    }

    return true;
}


void SimpleDocumentProcessor::destroy()
{
    delete this;
}

}
}
