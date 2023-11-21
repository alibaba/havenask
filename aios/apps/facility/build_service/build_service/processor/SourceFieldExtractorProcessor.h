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

#include <map>
#include <memory>
#include <string>
#include <vector>

#include "build_service/document/ExtendDocument.h"
#include "build_service/processor/DocumentProcessor.h"
#include "build_service/util/Log.h"
#include "build_service/util/SourceFieldExtractorUtil.h"

namespace build_service::processor {

using SourceFieldParamPtr = std::shared_ptr<util::SourceFieldExtractorUtil::SourceFieldParam>;
using SourceFieldParams = std::map<std::string, SourceFieldParamPtr>;

class SourceFieldExtractorProcessor : public DocumentProcessor
{
public:
    static constexpr const char* PROCESSOR_NAME = "SourceFieldExtractorProcessor";

public:
    SourceFieldExtractorProcessor();
    ~SourceFieldExtractorProcessor() = default;
    SourceFieldExtractorProcessor(const SourceFieldExtractorProcessor& other) = default;
    SourceFieldExtractorProcessor& operator=(const SourceFieldExtractorProcessor& other) = delete;

public:
    bool init(const DocProcessorInitParam& param) override;
    bool process(const std::shared_ptr<document::ExtendDocument>& document) override;
    void batchProcess(const std::vector<std::shared_ptr<document::ExtendDocument>>& docs) override;
    void destroy() override;
    SourceFieldExtractorProcessor* clone() override;
    std::string getDocumentProcessorName() const override { return PROCESSOR_NAME; }

private:
    std::shared_ptr<indexlib::util::Metric> _sourceFieldQpsMetric;
    // optimize for chain->clone()
    std::shared_ptr<SourceFieldParams> _fieldName2Param;

private:
    BS_LOG_DECLARE();
};

} // namespace build_service::processor
