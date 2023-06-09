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
#include "indexlib/index/inverted_index/truncate/SimpleTruncateWriterScheduler.h"

namespace indexlib::index {
AUTIL_LOG_SETUP(index, SimpleTruncateWriterScheduler);

Status SimpleTruncateWriterScheduler::PushWorkItem(autil::WorkItem* workItem)
{
    try {
        workItem->process();
    } catch (...) {
        workItem->destroy();
        AUTIL_LOG(ERROR, "work item process failed, unknown exception");
        return Status::InternalError("work item process failed");
    }
    workItem->destroy();
    return Status::OK();
}

Status SimpleTruncateWriterScheduler::WaitFinished() { return Status::OK(); }

} // namespace indexlib::index
