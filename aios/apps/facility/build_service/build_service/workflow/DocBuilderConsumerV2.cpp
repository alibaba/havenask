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
#include "build_service/workflow/DocBuilderConsumerV2.h"

#include "build_service/document/ProcessedDocument.h"
#include "indexlib/document/DocumentBatch.h"
#include "indexlib/document/DocumentIterator.h"
#include "indexlib/document/document.h"
#include "indexlib/framework/Locator.h"

namespace build_service { namespace workflow {

BS_LOG_SETUP(workflow, DocBuilderConsumerV2);

DocBuilderConsumerV2::DocBuilderConsumerV2(builder::BuilderV2* builder)
    : _builder(builder)
    , _endTimestamp(INVALID_TIMESTAMP)
{
}

DocBuilderConsumerV2::~DocBuilderConsumerV2() {}

// builder consume the processed doc
FlowError DocBuilderConsumerV2::consume(const document::ProcessedDocumentVecPtr& item)
{
    assert(item->size() == 1);
    /*
       AsyncBuilder build 把doc放进队列返回 FE_OK, 依赖下一篇doc把build异常信息透出
       在后续没有 doc 的场景无法透出异常状态
       把判断提前， 依赖没消息时发 skip doc 的行为
     */
    if (_builder->needReconstruct()) {
        return FE_RECONSTRUCT;
    }
    if (_builder->hasFatalError()) {
        return FE_FATAL;
    }
    const document::ProcessedDocumentPtr& processedDoc = (*item)[0];
    std::shared_ptr<indexlibv2::document::IDocumentBatch> batch = processedDoc->getDocumentBatch();
    if (!batch) {
        return FE_OK;
    }
    auto iter = indexlibv2::document::DocumentIterator<indexlibv2::document::IDocument>::Create(batch.get());
    while (iter->HasNext()) {
        auto doc = iter->Next();
        doc->SetLocator(processedDoc->getLocator());
    }

    // TODO(hanyao): refactor doc builder consumer, create document batch from parser/factory.

    if (_builder->hasFatalError()) {
        return FE_FATAL;
    }
    if (_builder->build(batch)) {
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

bool DocBuilderConsumerV2::getLocator(common::Locator& locator) const
{
    locator = _builder->getLastLocator();
    // locator.setSrc(lastLocator.GetSrc());
    // locator.setOffset(lastLocator.GetOffset());
    // locator.setProgress(lastLocator.GetProgress());
    // locator.setUserData(lastLocator.GetUserData());
    BS_LOG(INFO, "builder locator [%s]", locator.DebugString().c_str());
    return true;
}

bool DocBuilderConsumerV2::stop(FlowError lastError, StopOption stopOption)
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

void DocBuilderConsumerV2::notifyStop(StopOption stopOption)
{
    if (stopOption == SO_INSTANT) {
        _builder->notifyStop();
    }
}

}} // namespace build_service::workflow
