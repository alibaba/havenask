#include "build_service/processor/LineDataDocumentProcessor.h"

using namespace std;
using namespace autil;
using namespace build_service::document;

IE_NAMESPACE_USE(config);

namespace build_service {
namespace processor {
BS_LOG_SETUP(processor, LineDataDocumentProcessor);

const string LineDataDocumentProcessor::PROCESSOR_NAME = "LineDataDocumentProcessor";

LineDataDocumentProcessor::LineDataDocumentProcessor() {
}

LineDataDocumentProcessor::~LineDataDocumentProcessor() {
}

LineDataDocumentProcessor::LineDataDocumentProcessor(
        const LineDataDocumentProcessor& other)
    : DocumentProcessor(other)
    , _schema(other._schema)
{
}

bool LineDataDocumentProcessor::init(const DocProcessorInitParam &param) {
    _schema = param.schemaPtr;
    return true;
}

bool LineDataDocumentProcessor::process(
        const document::ExtendDocumentPtr &document)
{
    const FieldSchemaPtr &fieldSchema = _schema->GetFieldSchema();
    const AttributeSchemaPtr &attrSchema = _schema->GetAttributeSchema();
    if (!attrSchema) {
        BS_LOG(ERROR, "attribute schema is null");
        return false;
    }
    const ClassifiedDocumentPtr &classifiedDoc =
        document->getClassifiedDocument();
    const RawDocumentPtr &rawDoc =
        document->getRawDocument();
    for (auto it = fieldSchema->Begin(); it != fieldSchema->End(); ++it) {
        fieldid_t fieldId = (*it)->GetFieldId();
        if (!attrSchema->IsInAttribute(fieldId)) {
            continue;
        }
        const AttributeConfigPtr &attributeConfig =
            attrSchema->GetAttributeConfigByFieldId(fieldId);
        const string &attrName = attributeConfig->GetAttrName();
        if (rawDoc->exist(attrName)) {
            classifiedDoc->setAttributeField(fieldId,
                    rawDoc->getField(attrName));
        }
    }
    return true;
}

void LineDataDocumentProcessor::batchProcess(const vector<ExtendDocumentPtr> &docs) {
    for (size_t i = 0; i < docs.size(); i++) {
        if (!process(docs[i])) {
            docs[i]->setWarningFlag(ProcessorWarningFlag::PWF_PROCESS_FAIL_IN_BATCH);
        }
    }
}


}
}
