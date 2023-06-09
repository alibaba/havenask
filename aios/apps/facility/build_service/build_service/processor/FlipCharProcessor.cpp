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
#include "build_service/processor/FlipCharProcessor.h"

#include "indexlib/config/ITabletSchema.h"

using namespace std;
using namespace autil;

namespace build_service { namespace processor {

BS_LOG_SETUP(processor, FlipCharProcessor);

const string FlipCharProcessor::PROCESSOR_NAME = "FlipCharProcessor";

bool FlipCharProcessor::init(const DocProcessorInitParam& param)
{
    string positive = getValueFromKeyValueMap(param.parameters, "positive");
    if (!positive.empty() && positive.length() != 1) {
        BS_LOG(ERROR, "positive must be char");
        return false;
    }
    string negative = getValueFromKeyValueMap(param.parameters, "negative");
    if (!negative.empty() && negative.length() != 1) {
        BS_LOG(ERROR, "negative must be char");
        return false;
    }
    if ((positive.empty() && !negative.empty()) || (!positive.empty() && negative.empty())) {
        BS_LOG(ERROR, "not all empty");
        return false;
    }
    _positive = positive.empty() ? char(29) : positive[0];
    _negative = negative.empty() ? char(30) : negative[0];
    if (_positive != _negative) {
        const auto& fieldConfigs = param.schema->GetFieldConfigs();
        for (const auto& fieldConfig : fieldConfigs) {
            if (fieldConfig->IsMultiValue()) {
                _fieldVector.emplace_back(std::move(fieldConfig));
            }
        }
    }
    return true;
}

DocumentProcessor* FlipCharProcessor::clone() { return new FlipCharProcessor(*this); }

bool FlipCharProcessor::process(const document::ExtendDocumentPtr& document)
{
    if (_fieldVector.empty()) {
        return true;
    }
    auto rawDoc = document->getRawDocument();
    char positive = _positive;
    char negative = _negative;
    for (const auto& field : _fieldVector) {
        StringView key(field->GetFieldName());
        auto value = rawDoc->getField(key);
        if (value.empty()) {
            continue;
        }
        char* begin = (char*)value.data();
        char* end = begin + value.length();
        while (begin < end) {
            char c = *begin;
            if (c == positive) {
                *begin = negative;
            } else if (c == negative) {
                *begin = positive;
            }
            begin++;
        }
    }
    return true;
}

void FlipCharProcessor::batchProcess(const std::vector<document::ExtendDocumentPtr>& docs)
{
    if (_fieldVector.empty()) {
        return;
    }
    for (const auto& doc : docs) {
        process(doc);
    }
}

void FlipCharProcessor::destroy() { delete this; }

}} // namespace build_service::processor
