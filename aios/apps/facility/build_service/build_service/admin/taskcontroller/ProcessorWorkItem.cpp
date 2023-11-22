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
#include "build_service/admin/taskcontroller/ProcessorWorkItem.h"

#include <iosfwd>
#include <memory>

#include "build_service/util/ErrorLogCollector.h"

using namespace std;
using namespace build_service::util;

namespace build_service { namespace admin {
BS_LOG_SETUP(admin, ProcessorWorkItem);

ProcessorWorkItem::ProcessorWorkItem(const ProcessorWorkItem& other)
    : processorTask(new ProcessorTask(other.processorTask->getBuildId(), other.processorTask->getBuildStep(),
                                      other.processorTask->getTaskResourceManager()))
{
}

// bool ProcessorWorkItem::operator==(const ProcessorWorkItem &other) const {
//     if (!processorTask && !other.processorTask) {
//         return true;
//     }
//     if (processorTask && other.processorTask) {
//         return *processorTask == *other.processorTask;
//     }

//     return false;
// }

void ProcessorWorkItem::Jsonize(Jsonizable::JsonWrapper& json)
{
    processorTask->Jsonize(json);
    // json.Jsonize("is_latest", isLatest, isLatest);
}

}} // namespace build_service::admin
