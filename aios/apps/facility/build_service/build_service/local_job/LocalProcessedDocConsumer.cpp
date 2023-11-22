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
#include "build_service/local_job/LocalProcessedDocConsumer.h"

#include <assert.h>
#include <cstdint>
#include <iosfwd>
#include <memory>

#include "build_service/document/DocumentDefine.h"
#include "build_service/document/ProcessedDocument.h"

using namespace std;
using namespace build_service::document;
using namespace build_service::workflow;

namespace build_service { namespace local_job {

BS_LOG_SETUP(local_job, LocalProcessedDocConsumer);

LocalProcessedDocConsumer::LocalProcessedDocConsumer(ReduceDocumentQueue* queue,
                                                     const std::vector<int32_t>& reduceIdxTable)
    : _queue(queue)
    , _reduceIdxTable(reduceIdxTable)
{
}

LocalProcessedDocConsumer::~LocalProcessedDocConsumer() {}

FlowError LocalProcessedDocConsumer::consume(const document::ProcessedDocumentVecPtr& documentVec)
{
    assert(documentVec);
    assert(documentVec->size() == 1);
    const ProcessedDocumentPtr& document = (*documentVec)[0];
    if (!document->getDocument()) {
        return FE_OK;
    }
    const ProcessedDocument::DocClusterMetaVec& meta = document->getDocClusterMetaVec();
    if (meta.empty()) {
        return FE_OK;
    }
    int32_t reduceId = _reduceIdxTable[meta[0].hashId];
    if (reduceId == -1) {
        return FE_OK;
    }
    _queue->push(uint16_t(reduceId), document->getDocumentBatch());
    return FE_OK;
}

bool LocalProcessedDocConsumer::getLocator(common::Locator& locator) const { return true; }

bool LocalProcessedDocConsumer::stop(FlowError lastError, workflow::StopOption option) { return true; }

}} // namespace build_service::local_job
