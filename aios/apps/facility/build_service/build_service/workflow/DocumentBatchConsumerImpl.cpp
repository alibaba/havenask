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
#include "build_service/workflow/DocumentBatchConsumerImpl.h"

#include <cstdint>
#include <optional>

#include "indexlib/base/Constant.h"
#include "indexlib/document/IDocumentBatch.h"
#include "indexlib/indexlib.h"

namespace build_service { namespace workflow {

BS_LOG_SETUP(workflow, DocumentBatchConsumerImpl);

DocumentBatchConsumerImpl::DocumentBatchConsumerImpl() : _builder(nullptr), _endTimestamp(INVALID_TIMESTAMP) {}
DocumentBatchConsumerImpl::~DocumentBatchConsumerImpl() {}

bool DocumentBatchConsumerImpl::Init(builder::BuilderV2* builder)
{
    if (!builder) {
        BS_LOG(ERROR, "Init failed, builder is nullptr");
        return false;
    }
    _builder = builder;
    return true;
}

FlowError DocumentBatchConsumerImpl::consume(const std::shared_ptr<indexlibv2::document::IDocumentBatch>& docBatch)
{
    if (!docBatch) {
        return FE_OK;
    }

    if (_builder->hasFatalError()) {
        return FE_FATAL;
    }
    if (_builder->build(docBatch)) {
        return FE_OK;
    }
    if (_builder->hasFatalError()) {
        return FE_FATAL;
    }
    if (_builder->needReconstruct()) {
        return FE_RECONSTRUCT;
    }
    if (_builder->isSealed()) {
        return FE_DROPFLOW;
    }

    return FE_EXCEPTION;
}

bool DocumentBatchConsumerImpl::getLocator(common::Locator& locator) const
{
    locator = _builder->getLastLocator();
    return true;
}

bool DocumentBatchConsumerImpl::stop(FlowError lastError, StopOption stopOption)
{
    bool immediately = stopOption == SO_INSTANT;
    if (lastError == FE_EOF) {
        _builder->stop(_endTimestamp, /*needSeal*/ false, immediately);
    } else if (lastError == FE_SEALED) {
        _builder->stop(_endTimestamp, /*needSeal*/ true, immediately);
    } else {
        _builder->stop(std::nullopt, /*needSeal*/ false, immediately);
    }
    return !_builder->hasFatalError();
}

void DocumentBatchConsumerImpl::notifyStop(StopOption stopOption)
{
    if (stopOption == SO_INSTANT) {
        _builder->notifyStop();
    }
}

}} // namespace build_service::workflow
