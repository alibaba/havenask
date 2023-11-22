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
#pragma once

#include <stdint.h>
#include <string>
#include <vector>

#include "build_service/common_define.h"
#include "build_service/document/ExtendDocument.h"
#include "build_service/document/RawDocument.h"
#include "build_service/processor/DocumentProcessor.h"
#include "build_service/util/Log.h"

namespace build_service { namespace processor {

class DocTraceProcessor : public DocumentProcessor
{
public:
    static const std::string PROCESSOR_NAME;

public:
    DocTraceProcessor();
    ~DocTraceProcessor();
    DocTraceProcessor(const DocTraceProcessor& other);

private:
    DocTraceProcessor& operator=(const DocTraceProcessor&);

public:
    bool process(const document::ExtendDocumentPtr& document) override;
    void destroy() override;
    DocumentProcessor* clone() override;
    bool init(const DocProcessorInitParam& param) override;

    std::string getDocumentProcessorName() const override { return PROCESSOR_NAME; }

private:
    bool initMatchRules(const std::string& matchRules);
    bool match(const document::RawDocumentPtr& rawDoc);

private:
    struct MatchItem {
        std::string fieldName;
        std::string matchValue;
    };
    typedef std::vector<MatchItem> MatchRule;
    typedef std::vector<MatchRule> MatchRules;

    int64_t _matchCount;
    int64_t _sampleFreq;
    MatchRules _matchRules;
    std::string _traceField;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(DocTraceProcessor);

}} // namespace build_service::processor
