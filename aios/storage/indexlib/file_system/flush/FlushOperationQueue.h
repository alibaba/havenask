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
#pragma once

#include <future>
#include <memory>
#include <stddef.h>
#include <stdint.h>
#include <utility>
#include <vector>

#include "autil/Lock.h"
#include "autil/Log.h"
#include "indexlib/file_system/FileSystemDefine.h"
#include "indexlib/file_system/flush/Dumpable.h"
#include "indexlib/file_system/flush/FileFlushOperation.h"
#include "indexlib/file_system/flush/MkdirFlushOperation.h"

namespace indexlib { namespace file_system {
class StorageMetrics;
}} // namespace indexlib::file_system

namespace indexlib { namespace file_system {

// for now, we only cache parent dir,
// so mkdir for index/ attribute/ .. dirs will be called ..
// need to create deletionmap dir

class FlushOperationQueue : public Dumpable
{
public:
    using EntryMetaFreezeCallback = std::function<void(std::shared_ptr<FileFlushOperation>&& fileFlushOperation)>;

public:
    FlushOperationQueue(StorageMetrics* metrics = NULL) : _metrics(metrics), _flushMemoryUse(0) {}

    ~FlushOperationQueue() {}

public:
    FlushOperationQueue(const FlushOperationQueue&) = delete;
    FlushOperationQueue& operator=(const FlushOperationQueue&) = delete;

public:
    void Dump() override;

public:
    void PushBack(const FileFlushOperationPtr& flushOperation) noexcept;
    void PushBack(const MkdirFlushOperationPtr& flushOperation) noexcept;
    size_t Size() const noexcept;

    void SetPromise(std::unique_ptr<std::promise<bool>>&& flushPromise) noexcept
    {
        _flushPromise = std::move(flushPromise);
    }

    void SetEntryMetaFreezeCallback(EntryMetaFreezeCallback&& callback)
    {
        _entryMetaFreezeCallabck = std::move(callback);
    }

private:
    using MkdirFlushOperationVec = std::vector<MkdirFlushOperationPtr>;
    using FileFlushOperationVec = std::vector<FileFlushOperationPtr>;

    void RunFlushOperations(const FileFlushOperationVec& fileFlushOperation,
                            const MkdirFlushOperationVec& mkDirFlushOperation);
    void CleanUp();

private:
    StorageMetrics* _metrics;
    int64_t _flushMemoryUse;
    mutable autil::ThreadMutex _lock;
    FileFlushOperationVec _fileFlushOperations;
    MkdirFlushOperationVec _mkdirFlushOperations;
    std::unique_ptr<std::promise<bool>> _flushPromise;
    EntryMetaFreezeCallback _entryMetaFreezeCallabck;

private:
    AUTIL_LOG_DECLARE();
    friend class FlushOperationQueueTest;
};

typedef std::shared_ptr<FlushOperationQueue> FlushOperationQueuePtr;
}} // namespace indexlib::file_system
