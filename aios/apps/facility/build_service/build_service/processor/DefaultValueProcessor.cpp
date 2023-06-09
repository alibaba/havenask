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
#include "build_service/processor/DefaultValueProcessor.h"

#include "indexlib/config/ITabletSchema.h"

using namespace std;
using namespace build_service::document;
using namespace autil;

using namespace indexlib::config;

namespace build_service { namespace processor {
BS_LOG_SETUP(processor, DefaultValueProcessor);

const string DefaultValueProcessor::PROCESSOR_NAME = "DefaultValueProcessor";

DefaultValueProcessor::DefaultValueProcessor() : DocumentProcessor(ADD_DOC) {}

DefaultValueProcessor::DefaultValueProcessor(const DefaultValueProcessor& other)
    : DocumentProcessor(ADD_DOC)
    , _fieldDefaultValueMap(other._fieldDefaultValueMap)
{
}

DefaultValueProcessor::~DefaultValueProcessor() {}

bool DefaultValueProcessor::init(const DocProcessorInitParam& param)
{
    const auto& fieldConfigs = param.schema->GetFieldConfigs();
    for (const auto& fieldConfig : fieldConfigs) {
        StringView fieldName = StringView(fieldConfig->GetFieldName());
        StringView defaultAttrValue = StringView(fieldConfig->GetDefaultValue());
        if (!defaultAttrValue.empty()) {
            _fieldDefaultValueMap[fieldName] = defaultAttrValue;
        }
    }
    return true;
}

bool DefaultValueProcessor::process(const ExtendDocumentPtr& document)
{
    const RawDocumentPtr& rawDoc = document->getRawDocument();
    for (FieldDefaultValueMap::iterator iter = _fieldDefaultValueMap.begin(); iter != _fieldDefaultValueMap.end();
         iter++) {
        if (rawDoc->getField(iter->first).empty()) {
            rawDoc->setField(iter->first, iter->second);
        }
    }
    return true;
}

void DefaultValueProcessor::batchProcess(const vector<ExtendDocumentPtr>& docs)
{
    for (size_t i = 0; i < docs.size(); i++) {
        if (!process(docs[i])) {
            docs[i]->setWarningFlag(ProcessorWarningFlag::PWF_PROCESS_FAIL_IN_BATCH);
        }
    }
}

void DefaultValueProcessor::destroy() { delete this; }

DocumentProcessor* DefaultValueProcessor::clone() { return new DefaultValueProcessor(*this); }

}} // namespace build_service::processor
