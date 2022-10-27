#include "build_service/processor/DocTTLProcessor.h"
#include <autil/StringUtil.h>
#include <autil/TimeUtility.h>

using namespace std;
using namespace autil;
using namespace build_service::document;

namespace build_service {
namespace processor {
BS_LOG_SETUP(processor, DocTTLProcessor);
const std::string DocTTLProcessor::PROCESSOR_NAME = "DocTTLProcessor";
DocTTLProcessor::DocTTLProcessor()
    : DocumentProcessor(ADD_DOC | UPDATE_FIELD | DELETE_DOC | DELETE_SUB_DOC)
    , _ttlTime(0)
    , _ttlFieldName(DOC_TIME_TO_LIVE_IN_SECONDS)
{
}

DocTTLProcessor::~DocTTLProcessor() {
}

bool DocTTLProcessor::init(const DocProcessorInitParam &param) {
    string ttlTimeStr = getValueFromKeyValueMap(param.parameters, "ttl_in_second");
    if (ttlTimeStr.empty() || !StringUtil::fromString(ttlTimeStr, _ttlTime)) {
        BS_LOG(ERROR, "[ttl_in_second] must be specified as a integer");
        return false;
    }
    _ttlFieldName = param.schemaPtr->GetTTLFieldName();
    return true;
}

void DocTTLProcessor::batchProcess(const vector<ExtendDocumentPtr> &docs) {
    for (size_t i = 0; i < docs.size(); i++) {
        if (!process(docs[i])) {
            docs[i]->setWarningFlag(ProcessorWarningFlag::PWF_PROCESS_FAIL_IN_BATCH);
        }
    }
}

bool DocTTLProcessor::process(const document::ExtendDocumentPtr &document) {
    const document::RawDocumentPtr &rawDoc = document->getRawDocument();
    uint32_t docLiveTime = TimeUtility::currentTimeInSeconds() + _ttlTime;
    rawDoc->setField(_ttlFieldName, StringUtil::toString(docLiveTime));
    return true;
}

DocumentProcessor* DocTTLProcessor::clone() {
    return new DocTTLProcessor(*this);
}

}
}
