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
#include "build_service/processor/DupFieldProcessor.h"

using namespace std;
using namespace build_service::document;

namespace build_service { namespace processor {
BS_LOG_SETUP(processor, DupFieldProcessor);

const string DupFieldProcessor::PROCESSOR_NAME = "DupFieldProcessor";

DupFieldProcessor::DupFieldProcessor() : DocumentProcessor(ADD_DOC | UPDATE_FIELD | DELETE_DOC | DELETE_SUB_DOC) {}

DupFieldProcessor::~DupFieldProcessor() {}

bool DupFieldProcessor::init(const DocProcessorInitParam& param)
{
    _dupFields = param.parameters;
    return true;
}

bool DupFieldProcessor::process(const ExtendDocumentPtr& document)
{
    const RawDocumentPtr& rawDoc = document->getRawDocument();
    for (DupFieldMap::iterator it = _dupFields.begin(); it != _dupFields.end(); it++) {
        const string& to = it->first;
        const string& from = it->second;
        string fieldValue = rawDoc->getField(from);
        rawDoc->setField(to, fieldValue);
    }
    return true;
}

void DupFieldProcessor::batchProcess(const vector<ExtendDocumentPtr>& docs)
{
    for (size_t i = 0; i < docs.size(); i++) {
        if (!process(docs[i])) {
            docs[i]->setWarningFlag(ProcessorWarningFlag::PWF_PROCESS_FAIL_IN_BATCH);
        }
    }
}

void DupFieldProcessor::destroy() { delete this; }

DocumentProcessor* DupFieldProcessor::clone() { return new DupFieldProcessor(*this); }

}} // namespace build_service::processor
