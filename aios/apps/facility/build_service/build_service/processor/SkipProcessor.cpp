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
#include "build_service/processor/SkipProcessor.h"

#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"

using namespace std;
using namespace autil;
using namespace build_service::document;

namespace build_service { namespace processor {
BS_LOG_SETUP(processor, SkipProcessor);

const std::string SkipProcessor::PROCESSOR_NAME = "SkipProcessor";
SkipProcessor::SkipProcessor() : DocumentProcessor(ADD_DOC | DELETE_DOC | UPDATE_FIELD) {}

bool SkipProcessor::process(const ExtendDocumentPtr& document)
{
    document->getRawDocument()->setDocOperateType(SKIP_DOC);
    return true;
}

void SkipProcessor::batchProcess(const vector<ExtendDocumentPtr>& docs)
{
    for (size_t i = 0; i < docs.size(); i++) {
        if (!process(docs[i])) {
            docs[i]->setWarningFlag(ProcessorWarningFlag::PWF_PROCESS_FAIL_IN_BATCH);
        }
    }
}

DocumentProcessor* SkipProcessor::clone() { return new SkipProcessor(*this); }

bool SkipProcessor::init(const DocProcessorInitParam& param) { return true; }

std::string SkipProcessor::getDocumentProcessorName() const { return PROCESSOR_NAME; }

}} // namespace build_service::processor
