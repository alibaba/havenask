/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "build_service/processor/SleepProcessor.h"

#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"

using namespace std;
using namespace autil;
using namespace build_service::document;

namespace build_service { namespace processor {
BS_LOG_SETUP(processor, SleepProcessor);

const std::string SleepProcessor::SLEEP_PER_DOCUMENT = "sleep_per_document";
const std::string SleepProcessor::EXIT_TIME_INTERVAL = "exit_time_interval";
const std::string SleepProcessor::SLEEP_DTOR = "sleep_dtor";
const std::string SleepProcessor::PROCESSOR_NAME = "SleepProcessor";

SleepProcessor::SleepProcessor() : _sleepTimePerDoc(0), _sleepTimeDtor(0), _exitTimeInterval(-1) {}

SleepProcessor::~SleepProcessor() { sleep(_sleepTimeDtor); }

bool SleepProcessor::process(const ExtendDocumentPtr& document)
{
    sleep(_sleepTimePerDoc);
    if (_exitTimeInterval > 0) {
        auto currentTime = autil::TimeUtility::currentTimeInSeconds();
        if (currentTime - _startTimestamp > _exitTimeInterval) {
            std::cerr << "sleep processor exit" << std::endl;
            exit(0);
        }
    }
    return true;
}

void SleepProcessor::batchProcess(const vector<ExtendDocumentPtr>& docs)
{
    for (size_t i = 0; i < docs.size(); i++) {
        if (!process(docs[i])) {
            docs[i]->setWarningFlag(ProcessorWarningFlag::PWF_PROCESS_FAIL_IN_BATCH);
        }
    }
}

DocumentProcessor* SleepProcessor::clone() { return new SleepProcessor(*this); }

bool SleepProcessor::init(const DocProcessorInitParam& param)
{
    _startTimestamp = autil::TimeUtility::currentTimeInSeconds();
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

    KeyValueMap::const_iterator it3 = param.parameters.find(EXIT_TIME_INTERVAL);
    if (it3 != param.parameters.end() && StringUtil::strToInt64(it3->second.c_str(), _exitTimeInterval)) {
        BS_LOG(INFO, "exit interval [%ld]", _exitTimeInterval);
        return true;
    }
    return false;
}

std::string SleepProcessor::getDocumentProcessorName() const { return PROCESSOR_NAME; }

}} // namespace build_service::processor
