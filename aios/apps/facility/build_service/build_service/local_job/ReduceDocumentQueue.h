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
#ifndef ISEARCH_BS_PROCESSEDDOCUMENTRECORD_H
#define ISEARCH_BS_PROCESSEDDOCUMENTRECORD_H

#include "autil/Lock.h"
#include "build_service/common_define.h"
#include "build_service/document/ProcessedDocument.h"
#include "build_service/util/Log.h"
#include "build_service/util/StreamQueue.h"

namespace build_service { namespace local_job {

class ReduceDocumentQueue
{
    typedef util::StreamQueue<std::shared_ptr<indexlibv2::document::IDocumentBatch>> ReduceQueue;

public:
    ReduceDocumentQueue(uint16_t reduceCount)
    {
        for (uint16_t i = 0; i < reduceCount; i++) {
            _reduceQueues.push_back(std::shared_ptr<ReduceQueue>(new ReduceQueue));
        }
    }
    ReduceDocumentQueue() {}

private:
    ReduceDocumentQueue(const ReduceDocumentQueue&);
    ReduceDocumentQueue& operator=(const ReduceDocumentQueue&);

public:
    void push(uint16_t instanceId, const std::shared_ptr<indexlibv2::document::IDocumentBatch>& docBatch)
    {
        return _reduceQueues[instanceId]->push(docBatch);
    }

    bool pop(uint16_t instanceId, std::shared_ptr<indexlibv2::document::IDocumentBatch>& docBatch)
    {
        return _reduceQueues[instanceId]->pop(docBatch);
    }

    void setFinished()
    {
        for (size_t i = 0; i < _reduceQueues.size(); i++) {
            _reduceQueues[i]->setFinish();
        }
    }

private:
    std::vector<std::shared_ptr<ReduceQueue>> _reduceQueues;
};

}} // namespace build_service::local_job

#endif // ISEARCH_BS_PROCESSEDDOCUMENTRECORD_H
