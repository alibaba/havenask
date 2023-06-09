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

#include "build_service/common_define.h"
#include "build_service/processor/DocumentProcessor.h"
#include "build_service/util/Log.h"

namespace indexlibv2::config {
class FieldConfig;
}

namespace build_service { namespace processor {

class FlipCharProcessor final : public DocumentProcessor
{
public:
    bool init(const DocProcessorInitParam& param) override;
    DocumentProcessor* clone() override;
    bool process(const document::ExtendDocumentPtr& document) override;
    void batchProcess(const std::vector<document::ExtendDocumentPtr>& docs) override;
    void destroy() override;

    std::string getDocumentProcessorName() const override { return PROCESSOR_NAME; }

private:
    char _positive;
    char _negative;
    std::vector<std::shared_ptr<indexlibv2::config::FieldConfig>> _fieldVector;

public:
    static const std::string PROCESSOR_NAME;

private:
    BS_LOG_DECLARE();
};

}} // namespace build_service::processor
