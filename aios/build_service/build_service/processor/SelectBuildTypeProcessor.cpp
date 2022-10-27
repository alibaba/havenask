#include <autil/StringTokenizer.h>
#include "build_service/processor/SelectBuildTypeProcessor.h"

using namespace std;
using namespace autil;
using namespace build_service::config;
using namespace build_service::document;
namespace build_service {
namespace processor {
BS_LOG_SETUP(processor, SelectBuildTypeProcessor);

const string SelectBuildTypeProcessor::PROCESSOR_NAME = "SelectBuildTypeProcessor";
const string SelectBuildTypeProcessor::REALTIME_KEY_NAME = "need_mask_realtime_key";
const string SelectBuildTypeProcessor::REALTIME_VALUE_NAME = "need_mask_realtime_value";

// example:
// parameter {
//      build_type : realtime, inc
// }
bool SelectBuildTypeProcessor::init(const DocProcessorInitParam &param) {
    _notRealtimeKey = getValueFromKeyValueMap(param.parameters, REALTIME_KEY_NAME);
    if (_notRealtimeKey.empty()) {
        string errorMsg = "realtime_key is empty";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }

    _notRealtimeValue = getValueFromKeyValueMap(param.parameters, REALTIME_VALUE_NAME);
    if (_notRealtimeValue.empty()) {
        string errorMsg = "realtime_value is empty";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }

    return true;
}

bool SelectBuildTypeProcessor::process(const ExtendDocumentPtr& document)
{
    const ProcessedDocumentPtr &processedDocPtr = document->getProcessedDocument();
    const RawDocumentPtr &rawDocCumentPtr = document->getRawDocument();
    ProcessedDocument::DocClusterMetaVec metas = processedDocPtr->getDocClusterMetaVec();
    uint8_t buildType = 0;
    const string &notRealtimeValue = rawDocCumentPtr->getField(_notRealtimeKey);
    if (notRealtimeValue == _notRealtimeValue) {
        buildType |= ProcessedDocument::SWIFT_FILTER_BIT_REALTIME;
    }
    for (size_t i = 0; i < metas.size(); i++) {
        metas[i].buildType = buildType;
    }
    processedDocPtr->setDocClusterMetaVec(metas);
    return true;
}

void SelectBuildTypeProcessor::batchProcess(const vector<ExtendDocumentPtr> &docs) {
    for (size_t i = 0; i < docs.size(); i++) {
        if (!process(docs[i])) {
            docs[i]->setWarningFlag(ProcessorWarningFlag::PWF_PROCESS_FAIL_IN_BATCH);
        }
    }
}

void SelectBuildTypeProcessor::destroy()
{
    delete this;
}

}
}
