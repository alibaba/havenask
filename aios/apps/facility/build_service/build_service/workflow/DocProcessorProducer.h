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

#include "build_service/common/Locator.h"
#include "build_service/common_define.h"
#include "build_service/processor/Processor.h"
#include "build_service/util/Log.h"
#include "build_service/workflow/FlowError.h"
#include "build_service/workflow/Producer.h"
#include "build_service/workflow/StopOption.h"

namespace build_service { namespace workflow {

class DocProcessorProducer : public ProcessedDocProducer
{
public:
    DocProcessorProducer(processor::Processor* processor);
    ~DocProcessorProducer();

private:
    DocProcessorProducer(const DocProcessorProducer&);
    DocProcessorProducer& operator=(const DocProcessorProducer&);

public:
    FlowError produce(document::ProcessedDocumentVecPtr& processedDocVec) override;
    bool seek(const common::Locator& locator) override;
    bool stop(StopOption stopOption) override;
    void notifyStop(StopOption stopOption) override;

private:
    processor::Processor* _processor;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(DocProcessorProducer);

}} // namespace build_service::workflow
