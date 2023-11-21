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

#include "build_service/common/Locator.h"
#include "build_service/common_define.h"
#include "build_service/local_job/ReduceDocumentQueue.h"
#include "build_service/util/Log.h"
#include "build_service/workflow/FlowError.h"
#include "build_service/workflow/Producer.h"
#include "build_service/workflow/StopOption.h"

namespace build_service { namespace local_job {

class LocalProcessedDocProducer : public workflow::ProcessedDocProducer
{
public:
    LocalProcessedDocProducer(ReduceDocumentQueue* queue, uint64_t src, uint16_t instanceId);
    ~LocalProcessedDocProducer();

private:
    LocalProcessedDocProducer(const LocalProcessedDocProducer&);
    LocalProcessedDocProducer& operator=(const LocalProcessedDocProducer&);

public:
    workflow::FlowError produce(document::ProcessedDocumentVecPtr& processedDocVec) override;
    bool seek(const common::Locator& locator) override;
    bool stop(workflow::StopOption option) override;

private:
    ReduceDocumentQueue* _queue;
    uint64_t _src;
    uint16_t _instanceId;
    uint32_t _curDocCount;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(LocalProcessedDocProducer);

}} // namespace build_service::local_job
