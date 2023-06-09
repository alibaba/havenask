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

#include <functional>

#include "autil/Lock.h"
#include "indexlib/file_system/FileSystemDefine.h"
#include "indexlib/file_system/StorageMetrics.h"
#include "indexlib/file_system/file/FileNode.h"
#include "indexlib/util/memory_control/BlockMemoryQuotaController.h"
#include "indexlib/util/memory_control/SimpleMemoryQuotaController.h"

namespace indexlib { namespace file_system {

class ResourceFileNode : public FileNode
{
public:
    using HandleType = std::shared_ptr<void>;

public:
    ResourceFileNode(const util::BlockMemoryQuotaControllerPtr& memController) noexcept;
    ~ResourceFileNode() noexcept;

    ResourceFileNode(const ResourceFileNode&) = delete;
    ResourceFileNode& operator=(const ResourceFileNode&) = delete;

public:
    // called on creation
    void SetStorageMetrics(StorageMetrics* storageMetrics, FSMetricGroup metricGroup) noexcept
    {
        autil::ScopedLock lock(_lock);
        _storageMetrics = storageMetrics;
        SetMetricGroup(metricGroup);
    }

    void SetCleanCallback(std::function<void()>&& cleanFunction) noexcept
    {
        autil::ScopedLock lock(_lock);
        _CleanFunction = std::move(cleanFunction);
    }

public:
    HandleType GetResourceHandle() const noexcept;
    void* GetResource() noexcept;
    void Reset(void* resource, std::function<void(void*)> destroyFunction) noexcept;
    void UpdateMemoryUse(int64_t currentBytes) noexcept;
    int64_t GetMemoryUse() const { return _memoryUse; }
    bool Empty() const noexcept
    {
        autil::ScopedLock lock(_lock);
        return !_resource;
    }

public:
    FSFileType GetType() const noexcept override;
    size_t GetLength() const noexcept override;
    void* GetBaseAddress() const noexcept override;
    FSResult<size_t> Read(void* buffer, size_t length, size_t offset, ReadOption option) noexcept override;
    util::ByteSliceList* ReadToByteSliceList(size_t length, size_t offset, ReadOption option) noexcept override;
    FSResult<size_t> Write(const void* buffer, size_t length) noexcept override;
    FSResult<void> Close() noexcept override;
    bool ReadOnly() const noexcept override;

private:
    ErrorCode DoOpen(const std::string& path, FSOpenType openType, int64_t fileLength) noexcept override;
    ErrorCode DoOpen(const PackageOpenMeta& packageOpenMeta, FSOpenType openType) noexcept override;
    void SyncQuotaControl(bool forceSync) noexcept;

private:
    void Destroy() noexcept;

private:
    static constexpr size_t TRIGGER_UPDATE_MEMROY_DELTA_SIZE = 1024 * 1024;
    mutable autil::ThreadMutex _lock;
    HandleType _resource;
    std::function<void()> _CleanFunction;

    int64_t _memoryUse;
    StorageMetrics* _storageMetrics;
    util::SimpleMemoryQuotaControllerPtr _memController;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<ResourceFileNode> ResourceFileNodePtr;
}} // namespace indexlib::file_system
