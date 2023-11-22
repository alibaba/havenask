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
#include "build_service/task_base/UpdateLocatorTaskItem.h"

#include <iosfwd>
#include <string>

#include "alog/Logger.h"
#include "indexlib/framework/Locator.h"

using namespace std;

namespace build_service { namespace task_base {
BS_LOG_SETUP(task_base, UpdateLocatorTaskItem);

UpdateLocatorTaskItem::UpdateLocatorTaskItem(workflow::SwiftProcessedDocProducer* producer, builder::Builder* builder,
                                             indexlib::util::MetricProviderPtr metricProvider)
    : _producer(producer)
    , _builder(builder)
{
}

UpdateLocatorTaskItem::~UpdateLocatorTaskItem()
{
    common::Locator locator;
    if (_builder->getLastFlushedLocator(locator)) {
        if (locator != _locator) {
            if (_producer->updateCommittedCheckpoint(locator.GetOffset())) {
                _locator = locator;
            } else {
                BS_LOG(WARN, "updateCommittedCheckpoint[%s] failed", locator.DebugString().c_str());
            }
            _builder->storeBranchCkp();
        }
    }
}

void UpdateLocatorTaskItem::Run()
{
    common::Locator locator;
    if (_builder->getLastFlushedLocator(locator)) {
        if (locator != _locator) {
            if (_producer->updateCommittedCheckpoint(locator.GetOffset())) {
                _locator = locator;
                return;
            }
            BS_LOG(WARN, "updateCommittedCheckpoint[%s] failed", locator.DebugString().c_str());
            _builder->storeBranchCkp();
        }
    }
    if (_producer->needUpdateCommittedCheckpoint()) {
        _builder->tryDump();
    }
}

}} // namespace build_service::task_base
