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

#include "build_service/common/ExceedTsAction.h"
#include "build_service/common/Locator.h"
#include "build_service/common_define.h"
#include "build_service/document/RawDocument.h"
#include "build_service/util/Log.h"
#include "build_service/workflow/FlowError.h"
#include "build_service/workflow/StopOption.h"
#include "indexlib/document/IDocumentBatch.h"
#include "indexlib/document/document_factory_wrapper.h"

namespace build_service { namespace document {

class ProcessedDocument;
BS_TYPEDEF_PTR(ProcessedDocument);
typedef std::vector<ProcessedDocumentPtr> ProcessedDocumentVec;
BS_TYPEDEF_PTR(ProcessedDocumentVec);
typedef std::shared_ptr<indexlibv2::document::IDocumentBatch> IDocumentBatchPtr;

}} // namespace build_service::document

namespace build_service { namespace workflow {

class ProducerBase
{
public:
    ProducerBase() {}
    virtual ~ProducerBase() {}

public:
    virtual bool seek(const common::Locator& locator) = 0;
    virtual bool stop(StopOption stopOption) = 0;
    virtual int64_t suspendReadAtTimestamp(int64_t timestamp, common::ExceedTsAction action) { return 0; }
    virtual void notifyStop(StopOption stopOption) {}
};

template <typename T>
class Producer : public ProducerBase
{
public:
    Producer() : ProducerBase() {}
    virtual ~Producer() {}

public:
    virtual FlowError produce(T& item) = 0;

protected:
    indexlib::document::DocumentFactoryWrapperPtr _documentFactoryWrapper;
};

typedef Producer<document::RawDocumentPtr> RawDocProducer;
typedef Producer<document::ProcessedDocumentVecPtr> ProcessedDocProducer;
typedef Producer<document::IDocumentBatchPtr> DocumentBatchProducer;

}} // namespace build_service::workflow
