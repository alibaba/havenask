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
#include "build_service/document/RawDocument.h"
#include "build_service/util/Log.h"
#include "build_service/workflow/FlowError.h"
#include "build_service/workflow/StopOption.h"
#include "indexlib/document/IDocumentBatch.h"

namespace build_service { namespace document {

class ProcessedDocument;
BS_TYPEDEF_PTR(ProcessedDocument);
typedef std::vector<ProcessedDocumentPtr> ProcessedDocumentVec;
BS_TYPEDEF_PTR(ProcessedDocumentVec);

}} // namespace build_service::document

namespace build_service { namespace workflow {

class ConsumerBase
{
public:
    ConsumerBase() {}
    virtual ~ConsumerBase() {}

public:
    virtual bool getLocator(common::Locator& locator) const = 0;
    virtual bool stop(FlowError lastError, StopOption stopOption) = 0;
    virtual void SetEndBuildTimestamp(int64_t timestamp) {}
    virtual void notifyStop(StopOption stopOption) {}
};

template <typename T>
class Consumer : public ConsumerBase
{
public:
    Consumer() : ConsumerBase() {}
    virtual ~Consumer() {}

private:
    Consumer(const Consumer&);
    Consumer& operator=(const Consumer&);

public:
    virtual FlowError consume(const T& item) = 0;
};

typedef Consumer<document::RawDocumentPtr> RawDocConsumer;
typedef Consumer<document::ProcessedDocumentVecPtr> ProcessedDocConsumer;
typedef Consumer<std::shared_ptr<indexlibv2::document::IDocumentBatch>> DocumentBatchConsumer;

}} // namespace build_service::workflow
