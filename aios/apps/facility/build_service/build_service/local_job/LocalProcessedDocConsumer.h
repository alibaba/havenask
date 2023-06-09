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
#ifndef ISEARCH_BS_LOCALPROCESSEDDOCCONSUMER_H
#define ISEARCH_BS_LOCALPROCESSEDDOCCONSUMER_H

#include "build_service/common_define.h"
#include "build_service/local_job/ReduceDocumentQueue.h"
#include "build_service/util/Log.h"
#include "build_service/workflow/Consumer.h"

namespace build_service { namespace local_job {

class LocalProcessedDocConsumer : public workflow::ProcessedDocConsumer
{
public:
    LocalProcessedDocConsumer(ReduceDocumentQueue* queue, const std::vector<int32_t>& reduceIdxTable);
    ~LocalProcessedDocConsumer();

private:
    LocalProcessedDocConsumer(const LocalProcessedDocConsumer&);
    LocalProcessedDocConsumer& operator=(const LocalProcessedDocConsumer&);

public:
    workflow::FlowError consume(const document::ProcessedDocumentVecPtr& document) override;
    bool getLocator(common::Locator& locator) const override;
    bool stop(workflow::FlowError lastError, workflow::StopOption option) override;

private:
    ReduceDocumentQueue* _queue;
    std::vector<int32_t> _reduceIdxTable;

private:
    BS_LOG_DECLARE();
};

}} // namespace build_service::local_job

#endif // ISEARCH_BS_LOCALPROCESSEDDOCCONSUMER_H
