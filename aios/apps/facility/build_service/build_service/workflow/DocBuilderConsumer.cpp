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
#include "build_service/workflow/DocBuilderConsumer.h"

#include "build_service/document/ProcessedDocument.h"
using namespace std;
using namespace build_service::document;
using namespace indexlib::document;
using namespace indexlib::common;
namespace build_service { namespace workflow {

BS_LOG_SETUP(workflow, DocBuilderConsumer);

DocBuilderConsumer::DocBuilderConsumer(builder::Builder* builder) : _builder(builder), _endTimestamp(INVALID_TIMESTAMP)
{
}

DocBuilderConsumer::~DocBuilderConsumer() {}

// builder consume the processed doc
FlowError DocBuilderConsumer::consume(const ProcessedDocumentVecPtr& item)
{
    assert(item->size() == 1);
    const ProcessedDocumentPtr& processedDoc = (*item)[0];
    const DocumentPtr& indexDoc = std::dynamic_pointer_cast<Document>(processedDoc->getDocument());
    if (!indexDoc) {
        // Empty indexDoc happens in two cases:
        // 1. processedDoc is invalid(unlikely)
        // 2. processedDoc is internal doc(SKIP_DOC)
        // Here we flush builder to make sure builder can send signal to indexlib to trigger indexlib side
        // build flow. This can happen periodically if there is no user doc to build because SKIP_DOC is sent
        // periodically.
        _builder->flush();
        return FE_OK;
    }
    common::Locator locator = processedDoc->getLocator();
    BS_LOG(DEBUG, "build locator is: %s", locator.DebugString().c_str());
    indexDoc->SetLocator(locator.Serialize());

    if (_builder->hasFatalError()) {
        return FE_FATAL;
    }
    if (_builder->build(indexDoc)) {
        return FE_OK;
    }
    if (_builder->hasFatalError()) {
        return FE_FATAL;
    }

    return FE_EXCEPTION;
}

bool DocBuilderConsumer::getLocator(common::Locator& locator) const
{
    if (!_builder->getLastLocator(locator)) {
        string errorMsg = "get last locator failed";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    BS_LOG(INFO, "builder locator [%s]", locator.DebugString().c_str());
    return true;
}

bool DocBuilderConsumer::stop(FlowError lastError, StopOption stopOption)
{
    if (lastError == FE_EOF) {
        _builder->stop(_endTimestamp);
    } else {
        _builder->stop();
    }
    return !_builder->hasFatalError();
}

}} // namespace build_service::workflow
