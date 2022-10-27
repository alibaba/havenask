#include "build_service/processor/DupFieldProcessor.h"

using namespace std;
using namespace build_service::document;

namespace build_service {
namespace processor {
BS_LOG_SETUP(processor, DupFieldProcessor);

const string DupFieldProcessor::PROCESSOR_NAME = "DupFieldProcessor";

DupFieldProcessor::DupFieldProcessor()
    : DocumentProcessor(ADD_DOC | UPDATE_FIELD | DELETE_DOC | DELETE_SUB_DOC)
{
}

DupFieldProcessor::DupFieldProcessor(const DupFieldProcessor &other)
    : _dupFields(other._dupFields)
{
}

DupFieldProcessor::~DupFieldProcessor() {
}

bool DupFieldProcessor::init(const KeyValueMap &kvMap,
                             const IE_NAMESPACE(config)::IndexPartitionSchemaPtr &schemaPtr,
                             config::ResourceReader *resourceReader)
{
    _dupFields = kvMap;
    return true;
}

bool DupFieldProcessor::process(const ExtendDocumentPtr &document) {
    const RawDocumentPtr &rawDoc = document->getRawDocument();
    for (DupFieldMap::iterator it = _dupFields.begin();
         it != _dupFields.end(); it++)
    {
        const string &to = it->first;
        const string &from = it->second;
        string fieldValue = rawDoc->getField(from);
        rawDoc->setField(to, fieldValue);
    }
    return true;
}

void DupFieldProcessor::batchProcess(const vector<ExtendDocumentPtr> &docs) {
    for (size_t i = 0; i < docs.size(); i++) {
        if (!process(docs[i])) {
            docs[i]->setWarningFlag(ProcessorWarningFlag::PWF_PROCESS_FAIL_IN_BATCH);
        }
    }
}

void DupFieldProcessor::destroy() {
    delete this;
}

DocumentProcessor *DupFieldProcessor::clone() {
    return new DupFieldProcessor(*this);
}

}
}
