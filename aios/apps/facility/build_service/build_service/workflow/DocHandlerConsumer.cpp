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
#include "build_service/workflow/DocHandlerConsumer.h"

#include <cstddef>
#include <memory>
#include <vector>

using namespace std;

namespace build_service { namespace workflow {
BS_LOG_SETUP(workflow, DocHandlerConsumer);

DocHandlerConsumer::DocHandlerConsumer(ProcessedDocHandler* handler) : _handler(handler) {}

DocHandlerConsumer::~DocHandlerConsumer() {}

FlowError DocHandlerConsumer::consume(const document::ProcessedDocumentVecPtr& item)
{
    for (size_t i = 0; i < item->size(); i++) {
        _handler->consume((*item)[i]);
    }
    return FE_OK;
}

bool DocHandlerConsumer::getLocator(common::Locator& locator) const { return _handler->getLocator(locator); }

bool DocHandlerConsumer::stop(FlowError lastError, StopOption stopOption)
{
    _handler->stop(stopOption == StopOption::SO_INSTANT);
    return true;
}

}} // namespace build_service::workflow
