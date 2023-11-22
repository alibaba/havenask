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
#include "build_service/local_job/LocalProcessedDocProducer.h"

#include <iosfwd>
#include <memory>
#include <vector>

#include "alog/Logger.h"
#include "build_service/document/DocumentDefine.h"
#include "build_service/document/ProcessedDocument.h"
#include "indexlib/document/IDocumentBatch.h"
#include "indexlib/document/raw_document/raw_document_define.h"

using namespace std;
using namespace build_service::workflow;
using namespace build_service::document;
using namespace indexlib::document;

namespace build_service { namespace local_job {

BS_LOG_SETUP(local_job, LocalProcessedDocProducer);

LocalProcessedDocProducer::LocalProcessedDocProducer(ReduceDocumentQueue* queue, uint64_t src, uint16_t instanceId)
    : _queue(queue)
    , _src(src)
    , _instanceId(instanceId)
    , _curDocCount(0)
{
}

LocalProcessedDocProducer::~LocalProcessedDocProducer() {}

FlowError LocalProcessedDocProducer::produce(ProcessedDocumentVecPtr& docVec)
{
    std::shared_ptr<indexlibv2::document::IDocumentBatch> documentBatch;
    if (!_queue->pop(_instanceId, documentBatch)) {
        BS_LOG(INFO, "finish read all record! total record count[%d]", _curDocCount);
        return FE_EOF;
    }

    docVec.reset(new ProcessedDocumentVec());
    ProcessedDocumentPtr processedDoc(new ProcessedDocument);
    processedDoc->setLocator(common::Locator(_src, ++_curDocCount));
    processedDoc->setDocumentBatch(documentBatch);
    docVec->push_back(processedDoc);
    return FE_OK;
}

bool LocalProcessedDocProducer::seek(const common::Locator& locator) { return true; }

bool LocalProcessedDocProducer::stop(workflow::StopOption option) { return true; }

}} // namespace build_service::local_job
