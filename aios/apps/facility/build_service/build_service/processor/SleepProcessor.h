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
#include "build_service/processor/DocumentProcessor.h"
#include "build_service/util/Log.h"

namespace build_service { namespace processor {

class SleepProcessor : public DocumentProcessor
{
public:
    SleepProcessor();
    ~SleepProcessor();

private:
    SleepProcessor& operator=(const SleepProcessor&);

public:
    bool process(const document::ExtendDocumentPtr& document) override;
    void batchProcess(const std::vector<document::ExtendDocumentPtr>& docs) override;
    DocumentProcessor* clone() override;
    bool init(const DocProcessorInitParam& param) override;
    std::string getDocumentProcessorName() const override;

private:
    int64_t _sleepTimePerDoc;
    int64_t _sleepTimeDtor;
    int64_t _exitTimeInterval;
    int64_t _startTimestamp;

private:
    static const std::string SLEEP_PER_DOCUMENT;
    static const std::string SLEEP_DTOR;
    static const std::string EXIT_TIME_INTERVAL;

public:
    static const std::string PROCESSOR_NAME;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(SleepProcessor);

}} // namespace build_service::processor
