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

#include <set>
#include <stdint.h>
#include <string>
#include <vector>

#include "build_service/common_define.h"
#include "build_service/document/ExtendDocument.h"
#include "build_service/document/RawDocument.h"
#include "build_service/processor/DocumentProcessor.h"
#include "build_service/util/Log.h"

namespace build_service { namespace processor {

class FastSlowQueueProcessor : public DocumentProcessor
{
public:
    static constexpr const char* PROCESSOR_NAME = "FastSlowQueueProcessor";
    static constexpr const char* CONFIG_PATH = "config_path";
    static constexpr const char* NID = "nid";
    static constexpr const char* USER_ID = "user_id";

public:
    FastSlowQueueProcessor();
    ~FastSlowQueueProcessor() = default;
    FastSlowQueueProcessor(const FastSlowQueueProcessor& other) = default;
    FastSlowQueueProcessor& operator=(const FastSlowQueueProcessor& other) = delete;

public:
    bool init(const DocProcessorInitParam& param) override;
    bool process(const document::ExtendDocumentPtr& document) override;
    void batchProcess(const std::vector<document::ExtendDocumentPtr>& docs) override;
    void destroy() override;
    FastSlowQueueProcessor* clone() override;
    std::string getDocumentProcessorName() const override { return PROCESSOR_NAME; }

private:
    bool isFastQueueDoc(const document::RawDocumentPtr& rawDocument);

private:
    indexlib::util::MetricPtr _fastDocQpsMetric;
    std::set<int64_t> _userIdFilter;
    std::set<int64_t> _nidFilter;

private:
    BS_LOG_DECLARE();
    friend class FastSlowQueueProcessorTest;
};

BS_TYPEDEF_PTR(FastSlowQueueProcessor);

}} // namespace build_service::processor
