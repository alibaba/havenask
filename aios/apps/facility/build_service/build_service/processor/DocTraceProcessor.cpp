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
#include "build_service/processor/DocTraceProcessor.h"

#include <iosfwd>
#include <map>
#include <memory>
#include <utility>

#include "alog/Logger.h"
#include "autil/Span.h"
#include "autil/StringUtil.h"
#include "build_service/document/ClassifiedDocument.h"
#include "indexlib/base/Types.h"
#include "indexlib/document/RawDocument.h"
#include "indexlib/document/RawDocumentDefine.h"
#include "indexlib/document/normal/Field.h"
#include "indexlib/document/raw_document/raw_document_define.h"

using namespace std;
using namespace build_service::document;
using namespace indexlib::document;
using namespace autil;

namespace build_service { namespace processor {
BS_LOG_SETUP(processor, DocTraceProcessor);

const string DocTraceProcessor::PROCESSOR_NAME = "DocTraceProcessor";

DocTraceProcessor::DocTraceProcessor()
    : DocumentProcessor(ADD_DOC | UPDATE_FIELD | DELETE_DOC | DELETE_SUB_DOC)
    , _matchCount(0)
    , _sampleFreq(1)
{
}

DocTraceProcessor::DocTraceProcessor(const DocTraceProcessor& other)
    : _matchCount(other._matchCount)
    , _sampleFreq(other._sampleFreq)
    , _matchRules(other._matchRules)
    , _traceField(other._traceField)
{
}

DocTraceProcessor::~DocTraceProcessor() {}

bool DocTraceProcessor::init(const DocProcessorInitParam& param)
{
    const auto& kvMap = param.parameters;
    auto iter = kvMap.find("match_rule");
    if (iter != kvMap.end()) {
        if (!initMatchRules(iter->second)) {
            BS_LOG(ERROR, "match rules: %s, not valid", iter->second.c_str());
            return false;
        }
    }
    iter = kvMap.find("sample_freq");
    if (iter != kvMap.end()) {
        if (!StringUtil::fromString(iter->second, _sampleFreq)) {
            BS_LOG(ERROR, "sample freq [%s] not number", iter->second.c_str());
            return false;
        }
    }

    iter = kvMap.find("trace_field");
    if (iter == kvMap.end()) {
        BS_LOG(ERROR, "no trace field");
        return false;
    }
    _traceField = iter->second;
    return true;
}

bool DocTraceProcessor::initMatchRules(const string& matchRules)
{
    vector<string> matchRuleStrs;
    StringUtil::fromString(matchRules, matchRuleStrs, ";");
    for (auto& matchRuleStr : matchRuleStrs) {
        vector<string> matchRule;
        StringUtil::fromString(matchRuleStr, matchRule, ",");
        MatchRule targetMatchRule;
        targetMatchRule.reserve(matchRule.size());
        for (auto& matchItemStr : matchRule) {
            vector<string> matchItem;
            StringUtil::fromString(matchItemStr, matchItem, "=");
            if (matchItem.size() != 2) {
                return false;
            }
            MatchItem targetMatchItem;
            targetMatchItem.fieldName = matchItem[0];
            targetMatchItem.matchValue = matchItem[1];
            targetMatchRule.push_back(targetMatchItem);
        }
        _matchRules.push_back(targetMatchRule);
    }
    return true;
}

bool DocTraceProcessor::process(const document::ExtendDocumentPtr& document)
{
    auto rawDoc = document->getRawDocument();
    if (!match(rawDoc)) {
        return true;
    }
    _matchCount++;
    if (_matchCount % _sampleFreq != 0) {
        return true;
    }
    string fieldValue = rawDoc->getField(_traceField);
    rawDoc->setField(BUILTIN_KEY_TRACE_ID, fieldValue);
    return true;
}

bool DocTraceProcessor::match(const RawDocumentPtr& rawDoc)
{
    for (auto& matchRule : _matchRules) {
        bool match = true;
        for (auto& matchItem : matchRule) {
            string matchValue = rawDoc->getField(matchItem.fieldName);
            if (matchValue != matchItem.matchValue) {
                match = false;
                break;
            }
        }
        if (match) {
            return true;
        }
    }
    return _matchRules.size() == 0;
}

void DocTraceProcessor::destroy() { delete this; }

DocumentProcessor* DocTraceProcessor::clone() { return new DocTraceProcessor(*this); }

}} // namespace build_service::processor
