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
#include "build_service/workflow/RawDocBuilderConsumer.h"

#include "build_service/document/ProcessedDocument.h"
using namespace std;
using namespace build_service::document;
using namespace indexlib::document;
using namespace indexlib::common;
namespace build_service { namespace workflow {

BS_LOG_SETUP(workflow, RawDocBuilderConsumer);

RawDocBuilderConsumer::RawDocBuilderConsumer(builder::Builder* builder)
    : _builder(builder)
    , _endTimestamp(INVALID_TIMESTAMP)
{
}

RawDocBuilderConsumer::~RawDocBuilderConsumer() {}

FlowError RawDocBuilderConsumer::consume(const document::RawDocumentPtr& item)
{
    if (unlikely(!item)) {
        return FE_OK;
    }
    BS_LOG(DEBUG, "build locator is: %s", item->getLocator().DebugString().c_str());
    if (_builder->hasFatalError()) {
        return FE_FATAL;
    }
    const auto& document = std::dynamic_pointer_cast<indexlib::document::Document>(item);
    if (!document) {
        BS_LOG(ERROR, "cast to document failed");
        return FE_FATAL;
    }
    if (_builder->build(document)) {
        return FE_OK;
    }
    if (_builder->hasFatalError()) {
        return FE_FATAL;
    }

    return FE_EXCEPTION;
}

bool RawDocBuilderConsumer::getLocator(common::Locator& locator) const
{
    if (!_builder->getLastLocator(locator)) {
        string errorMsg = "get last locator failed";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    BS_LOG(INFO, "builder locator [%s]", locator.DebugString().c_str());
    return true;
}

bool RawDocBuilderConsumer::stop(FlowError lastError, StopOption stopOption)
{
    if (lastError == FE_EOF) {
        _builder->stop(_endTimestamp);
    } else {
        _builder->stop();
    }
    return !_builder->hasFatalError();
}

}} // namespace build_service::workflow
