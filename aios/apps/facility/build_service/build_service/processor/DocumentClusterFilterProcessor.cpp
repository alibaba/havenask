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
#include "build_service/processor/DocumentClusterFilterProcessor.h"

#include <cstddef>
#include <ext/alloc_traits.h>
#include <memory>

#include "alog/Logger.h"
#include "autil/Span.h"
#include "autil/StringTokenizer.h"
#include "autil/StringUtil.h"
#include "build_service/document/ClassifiedDocument.h"
#include "indexlib/base/Types.h"
#include "indexlib/document/RawDocument.h"

using namespace std;
using namespace autil;
using namespace build_service::document;

namespace build_service { namespace processor {
BS_LOG_SETUP(processor, DocumentClusterFilterProcessor);

const string DocumentClusterFilterProcessor::PROCESSOR_NAME = "DocumentClusterFilterProcessor";
const string DocumentClusterFilterProcessor::FILTER_RULE = "filter_rule";

DocumentClusterFilterProcessor::DocumentClusterFilterProcessor()
    : DocumentProcessor(ADD_DOC | UPDATE_FIELD | DELETE_DOC | DELETE_SUB_DOC)
    , _value(0)
{
}

DocumentClusterFilterProcessor::~DocumentClusterFilterProcessor() {}

bool DocumentClusterFilterProcessor::init(const DocProcessorInitParam& param)
{
    auto filterRule = getValueFromKeyValueMap(param.parameters, FILTER_RULE);
    if (filterRule.empty()) {
        BS_LOG(ERROR, "[%s] is empty in DocumentClusterFilterProcessor", FILTER_RULE.c_str());
        return false;
    }
    if (!parseRule(filterRule)) {
        BS_LOG(ERROR, "parse filter rule failed");
        return false;
    }
    return true;
}

bool DocumentClusterFilterProcessor::parseRule(const string& ruleStr)
{
    // TODO: support complex syntax: AND, OR ...
    StringTokenizer st(ruleStr, " ", StringTokenizer::TOKEN_IGNORE_EMPTY | StringTokenizer::TOKEN_TRIM);
    if (st.getNumTokens() != 3) {
        BS_LOG(ERROR, "[%s] not supported", ruleStr.c_str());
        return false;
    }

    if (!StringUtil::numberFromString(st[2], _value)) {
        BS_LOG(ERROR, "[%s] is not integer", st[2].c_str());
        return false;
    }
    _fieldName = st[0];
    if (st[1] == ">") {
        _op = [](int64_t x, int64_t y) { return x > y; };
    } else if (st[1] == "<") {
        _op = [](int64_t x, int64_t y) { return x < y; };
    } else if (st[1] == "==") {
        _op = [](int64_t x, int64_t y) { return x == y; };
    } else if (st[1] == "<=") {
        _op = [](int64_t x, int64_t y) { return x <= y; };
    } else if (st[1] == ">=") {
        _op = [](int64_t x, int64_t y) { return x >= y; };
    } else if (st[1] == "!=") {
        _op = [](int64_t x, int64_t y) { return x != y; };
    } else {
        BS_LOG(ERROR, "unsupported op[%s]", st[1].c_str());
        return false;
    }
    return true;
}

DocumentProcessor* DocumentClusterFilterProcessor::clone() { return new DocumentClusterFilterProcessor(*this); }

bool DocumentClusterFilterProcessor::process(const document::ExtendDocumentPtr& document)
{
    const auto& rawDoc = document->getRawDocument();
    const auto& fieldValue = rawDoc->getField(_fieldName);
    if (fieldValue.empty()) {
        BS_LOG(DEBUG, "[%s] not found in raw document", _fieldName.c_str());
        return true;
    }
    int64_t value = 0;
    if (!StringUtil::numberFromString(fieldValue, value)) {
        BS_LOG(ERROR, "field [%s] value [%s] is not integer", _fieldName.c_str(), fieldValue.c_str());
        return false;
    }
    if (_op(value, _value)) {
        rawDoc->setDocOperateType(SKIP_DOC);
    }
    return true;
}

void DocumentClusterFilterProcessor::batchProcess(const vector<ExtendDocumentPtr>& docs)
{
    for (size_t i = 0; i < docs.size(); i++) {
        if (!process(docs[i])) {
            docs[i]->setWarningFlag(ProcessorWarningFlag::PWF_PROCESS_FAIL_IN_BATCH);
        }
    }
}

}} // namespace build_service::processor
