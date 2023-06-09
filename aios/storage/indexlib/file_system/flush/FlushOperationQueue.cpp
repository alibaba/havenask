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
#include "indexlib/file_system/flush/FlushOperationQueue.h"

#include <cstring>
#include <ext/alloc_traits.h>
#include <functional>
#include <iosfwd>

#include "alog/Logger.h"
#include "autil/CommonMacros.h"
#include "autil/Lock.h"
#include "autil/TimeUtility.h"
#include "fslib/common/common_type.h"
#include "indexlib/file_system/StorageMetrics.h"
#include "indexlib/file_system/flush/DirOperationCache.h"

using namespace std;
using namespace autil;
using namespace indexlib::util;

namespace indexlib { namespace file_system {
AUTIL_LOG_SETUP(indexlib.file_system, FlushOperationQueue);

void FlushOperationQueue::Dump()
{
    ScopedLock lock(_lock);
    AUTIL_LOG(INFO,
              "Start Flush. File Operations total: %lu"
              " Dir Operations total: %lu",
              _fileFlushOperations.size(), _mkdirFlushOperations.size());

    try {
        RunFlushOperations(_fileFlushOperations, _mkdirFlushOperations);
    } catch (...) {
        CleanUp();
        if (_flushPromise) {
            _flushPromise->set_exception(std::current_exception());
        }
        throw;
    }

    CleanUp();
    if (_flushPromise) {
        _flushPromise->set_value(true);
    }
    AUTIL_LOG(INFO, "End Flush.");
}

void FlushOperationQueue::CleanUp()
{
    for (size_t i = 0; i < _fileFlushOperations.size(); ++i) {
        assert(_entryMetaFreezeCallabck);
        _entryMetaFreezeCallabck(std::move(_fileFlushOperations[i]));
    }

    _fileFlushOperations.clear();
    _mkdirFlushOperations.clear();

    if (_metrics) {
        _metrics->DecreaseFlushMemoryUse(_flushMemoryUse);
    }
    _flushMemoryUse = 0;
}

void FlushOperationQueue::PushBack(const FileFlushOperationPtr& flushOperation) noexcept
{
    ScopedLock lock(_lock);
    _fileFlushOperations.push_back(flushOperation);

    size_t flushMemUse = flushOperation->GetFlushMemoryUse();
    if (_metrics) {
        _metrics->IncreaseFlushMemoryUse(flushMemUse);
    }
    _flushMemoryUse += flushMemUse;
}

void FlushOperationQueue::PushBack(const MkdirFlushOperationPtr& flushOperation) noexcept
{
    ScopedLock lock(_lock);
    _mkdirFlushOperations.push_back(flushOperation);
}

size_t FlushOperationQueue::Size() const noexcept { return _fileFlushOperations.size() + _mkdirFlushOperations.size(); }

void FlushOperationQueue::RunFlushOperations(const FileFlushOperationVec& fileFlushOperation,
                                             const MkdirFlushOperationVec& mkDirFlushOperation)
{
    int64_t flushBeginTime = autil::TimeUtility::currentTime();
    DirOperationCache dirOperationCache;
    for (size_t i = 0; i < fileFlushOperation.size(); ++i) {
        // optimize for dfs path, add path to dir cache but not really do mkdir
        dirOperationCache.MkParentDirIfNecessary(fileFlushOperation[i]->GetDestPath());
    }

    for (size_t i = 0; i < mkDirFlushOperation.size(); ++i) {
        // dfs scene: will mkdir only for empty path
        dirOperationCache.Mkdir(mkDirFlushOperation[i]->GetDestPath());
    }

    for (size_t i = 0; i < fileFlushOperation.size(); ++i) {
        THROW_IF_FS_ERROR(fileFlushOperation[i]->Execute(), "");
    }

    int64_t flushEndTime = autil::TimeUtility::currentTime();
    AUTIL_LOG(INFO, "flush operations done, time[%lds]", (flushEndTime - flushBeginTime) / 1000 / 1000);
}
}} // namespace indexlib::file_system
