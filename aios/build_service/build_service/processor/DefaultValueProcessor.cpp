#include "build_service/processor/DefaultValueProcessor.h"

using namespace std;
using namespace build_service::document;
using namespace autil;

IE_NAMESPACE_USE(config);

namespace build_service {
namespace processor {
BS_LOG_SETUP(processor, DefaultValueProcessor);

const string DefaultValueProcessor::PROCESSOR_NAME = "DefaultValueProcessor";

DefaultValueProcessor::DefaultValueProcessor()
    : DocumentProcessor(ADD_DOC | UPDATE_FIELD | DELETE_DOC | DELETE_SUB_DOC)
{
}

DefaultValueProcessor::DefaultValueProcessor(const DefaultValueProcessor &other)
    : _fieldDefaultValueMap(other._fieldDefaultValueMap)
{
}

DefaultValueProcessor::~DefaultValueProcessor() {
}

bool DefaultValueProcessor::init(const KeyValueMap &kvMap,
                                 const IE_NAMESPACE(config)::IndexPartitionSchemaPtr &schemaPtr,
                                 config::ResourceReader *resourceReader)
{
    const FieldSchemaPtr & fieldSchema = schemaPtr->GetFieldSchema();
    for (FieldSchema::Iterator iter = fieldSchema->Begin(); iter != fieldSchema->End(); iter++) {
        ConstString fieldName = ConstString((*iter)->GetFieldName());
        ConstString defaultAttrValue = ConstString((*iter)->GetDefaultValue());
        if (!defaultAttrValue.empty()) {
            _fieldDefaultValueMap[fieldName] = defaultAttrValue;
        }
    }
    return true;
}

bool DefaultValueProcessor::process(const ExtendDocumentPtr &document) {
    const RawDocumentPtr &rawDoc = document->getRawDocument();
    for (FieldDefaultValueMap::iterator iter = _fieldDefaultValueMap.begin();
         iter != _fieldDefaultValueMap.end(); iter++)
    {
        if (rawDoc->getField(iter->first).empty()) {
            rawDoc->setField(iter->first, iter->second);
        }
    }
    return true;
}

void DefaultValueProcessor::batchProcess(const vector<ExtendDocumentPtr> &docs) {
    for (size_t i = 0; i < docs.size(); i++) {
        if (!process(docs[i])) {
            docs[i]->setWarningFlag(ProcessorWarningFlag::PWF_PROCESS_FAIL_IN_BATCH);
        }
    }
}

void DefaultValueProcessor::destroy() {
    delete this;
}

DocumentProcessor *DefaultValueProcessor::clone() {
    return new DefaultValueProcessor(*this);
}

}
}
