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
#include "build_service/workflow/DocProcessorConsumer.h"

using namespace std;
namespace build_service { namespace workflow {

BS_LOG_SETUP(workflow, DocProcessorConsumer);

DocProcessorConsumer::DocProcessorConsumer(processor::Processor* processor) : _processor(processor) {}

DocProcessorConsumer::~DocProcessorConsumer() {}

// processor consume raw doc (to processed doc)
FlowError DocProcessorConsumer::consume(const document::RawDocumentPtr& item)
{
    if (_processor->isDropped()) {
        return FE_DROPFLOW;
    }
    _processor->processDoc(item);
    return FE_OK;
}

bool DocProcessorConsumer::getLocator(common::Locator& locator) const
{
    locator = common::Locator();
    return true;
}

bool DocProcessorConsumer::stop(FlowError lastError, StopOption stopOption)
{
    _processor->stop(stopOption == SO_INSTANT, lastError == FE_SEALED);
    return true;
}

void DocProcessorConsumer::notifyStop(StopOption stopOption)
{
    if (stopOption == SO_INSTANT) {
        _processor->drop();
    }
}

}} // namespace build_service::workflow
