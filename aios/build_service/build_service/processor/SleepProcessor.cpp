#include "build_service/processor/SleepProcessor.h"
#include <autil/StringUtil.h>

using namespace std;
using namespace autil;
using namespace build_service::document;

namespace build_service {
namespace processor {
BS_LOG_SETUP(processor, SleepProcessor);

const std::string SleepProcessor::SLEEP_PER_DOCUMENT = "sleep_per_document";
const std::string SleepProcessor::SLEEP_DTOR = "sleep_dtor";
const std::string SleepProcessor::PROCESSOR_NAME = "SleepProcessor";

SleepProcessor::SleepProcessor()
    : _sleepTimePerDoc(0)
    , _sleepTimeDtor(0)
{
}

SleepProcessor::~SleepProcessor() {
    sleep(_sleepTimeDtor);
}

bool SleepProcessor::process(const ExtendDocumentPtr &document) {
    sleep(_sleepTimePerDoc);
    return true;
}

void SleepProcessor::batchProcess(const vector<ExtendDocumentPtr> &docs) {
    for (size_t i = 0; i < docs.size(); i++) {
        if (!process(docs[i])) {
            docs[i]->setWarningFlag(ProcessorWarningFlag::PWF_PROCESS_FAIL_IN_BATCH);
        }
    }
}

DocumentProcessor* SleepProcessor::clone() {
    return new SleepProcessor(*this);
}

bool SleepProcessor::init(const DocProcessorInitParam &param) {
    KeyValueMap::const_iterator it1 = param.parameters.find(SLEEP_PER_DOCUMENT);
    if (it1 != param.parameters.end() && StringUtil::strToInt64(it1->second.c_str(), _sleepTimePerDoc)) {
        BS_LOG(INFO, "sleep per doc[%ld]", _sleepTimePerDoc);
        return true;
    }
    KeyValueMap::const_iterator it2 = param.parameters.find(SLEEP_DTOR);
    if (it2 != param.parameters.end() && StringUtil::strToInt64(it2->second.c_str(), _sleepTimeDtor)) {
        BS_LOG(INFO, "sleep dtor[%ld]", _sleepTimeDtor);
        return true;
    }

    return false;
}

std::string SleepProcessor::getDocumentProcessorName() const {
    return PROCESSOR_NAME;
}

}
}
