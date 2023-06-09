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
#include "build_service/processor/SourceDocumentAccessaryFieldProcessor.h"

#include "autil/StringUtil.h"

using namespace std;
using namespace autil;
using namespace build_service::document;

namespace build_service { namespace processor {
BS_LOG_SETUP(processor, SourceDocumentAccessaryFieldProcessor);

const std::string SourceDocumentAccessaryFieldProcessor::PROCESSOR_NAME = "SourceDocumentAccessaryFieldProcessor";

SourceDocumentAccessaryFieldProcessor::SourceDocumentAccessaryFieldProcessor()
    : DocumentProcessor(ADD_DOC | UPDATE_FIELD | DELETE_DOC | DELETE_SUB_DOC)
{
}

SourceDocumentAccessaryFieldProcessor::SourceDocumentAccessaryFieldProcessor(
    const SourceDocumentAccessaryFieldProcessor& other)
    : _accessaryFields(other._accessaryFields)
{
}

SourceDocumentAccessaryFieldProcessor::~SourceDocumentAccessaryFieldProcessor() {}

bool SourceDocumentAccessaryFieldProcessor::process(const ExtendDocumentPtr& document)
{
    if (!document) {
        return false;
    }

    const ClassifiedDocumentPtr& classifiedDoc = document->getClassifiedDocument();
    const RawDocumentPtr& rawDoc = document->getRawDocument();
    if (!classifiedDoc || !rawDoc) {
        return false;
    }
    classifiedDoc->sourceDocumentAppendAccessaryFields(_accessaryFields, rawDoc);
    return true;
}

void SourceDocumentAccessaryFieldProcessor::batchProcess(const vector<ExtendDocumentPtr>& docs)
{
    for (size_t i = 0; i < docs.size(); i++) {
        if (!process(docs[i])) {
            docs[i]->setWarningFlag(ProcessorWarningFlag::PWF_PROCESS_FAIL_IN_BATCH);
        }
    }
}

DocumentProcessor* SourceDocumentAccessaryFieldProcessor::clone()
{
    return new SourceDocumentAccessaryFieldProcessor(*this);
}

bool SourceDocumentAccessaryFieldProcessor::init(const DocProcessorInitParam& param)
{
    string delim = ";";
    auto it = param.parameters.find("delimeter");
    if (it != param.parameters.end()) {
        delim = it->second;
    }

    it = param.parameters.find("fields");
    if (it != param.parameters.end()) {
        BS_LOG(INFO, "source document append accessary fields [%s], delimeter [%s] ", it->second.c_str(),
               delim.c_str());
        autil::StringUtil::fromString(it->second, _accessaryFields, delim);
        return !_accessaryFields.empty();
    }
    BS_LOG(ERROR, "key [fields] is required in key-value parameters.");
    return false;
}

std::string SourceDocumentAccessaryFieldProcessor::getDocumentProcessorName() const { return PROCESSOR_NAME; }

}} // namespace build_service::processor
