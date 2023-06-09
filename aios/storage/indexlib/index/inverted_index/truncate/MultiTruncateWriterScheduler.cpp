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
#include "indexlib/index/inverted_index/truncate/MultiTruncateWriterScheduler.h"

namespace indexlib::index {
AUTIL_LOG_SETUP(index, MultiTruncateWriterScheduler);

MultiTruncateWriterScheduler::MultiTruncateWriterScheduler(uint32_t threadNum, uint32_t queueSize)
    : _threadPool(threadNum, queueSize, true)
    , _started(false)
{
}

Status MultiTruncateWriterScheduler::PushWorkItem(autil::WorkItem* workItem)
{
    if (!_started) {
        _threadPool.start("TruncateIndex");
        _started = true;
    }
    try {
        if (_threadPool.pushWorkItem(workItem) != autil::ThreadPool::ERROR_NONE) {
            autil::ThreadPool::dropItemIgnoreException(workItem);
            AUTIL_LOG(ERROR, "push work item failed");
            return Status::InternalError("push work item failed");
        }
    } catch (...) {
        AUTIL_LOG(ERROR, "push work item failed, unknown exception");
        return Status::InternalError("push work item exception");
    }
    return Status::OK();
}

Status MultiTruncateWriterScheduler::WaitFinished()
{
    if (_started) {
        try {
            _threadPool.waitFinish();
        } catch (...) {
            AUTIL_LOG(ERROR, "wait finished failed, unknown exception");
            return Status::InternalError("wait finished failed");
        }
    }
    return Status::OK();
}

} // namespace indexlib::index
