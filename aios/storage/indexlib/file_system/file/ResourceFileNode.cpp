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
#include "indexlib/file_system/file/ResourceFileNode.h"

#include "indexlib/util/Exception.h"
#include "indexlib/util/metrics/MetricReporter.h"

namespace indexlib { namespace file_system {

AUTIL_LOG_SETUP(indexlib.file_system, ResourceFileNode);

ResourceFileNode::ResourceFileNode(const util::BlockMemoryQuotaControllerPtr& memController) noexcept
    : _resource(nullptr)
    , _memoryUse(0)
    , _storageMetrics(nullptr)
{
    _memController.reset(new util::SimpleMemoryQuotaController(memController));
}

ResourceFileNode::~ResourceFileNode() noexcept
{
    if (_CleanFunction) {
        _CleanFunction();
        _CleanFunction = nullptr;
    }
    Destroy();
}

void ResourceFileNode::Destroy() noexcept
{
    if (!_resource) {
        return;
    }
    _resource.reset();
    _memoryUse = 0;
    SyncQuotaControl(true);
}

ResourceFileNode::HandleType ResourceFileNode::GetResourceHandle() const noexcept
{
    autil::ScopedLock lock(_lock);
    return _resource;
}

void* ResourceFileNode::GetResource() noexcept
{
    autil::ScopedLock lock(_lock);
    if (_resource) {
        return _resource.get();
    }
    return nullptr;
}

void ResourceFileNode::Reset(void* resource, std::function<void(void*)> destroyFunction) noexcept
{
    autil::ScopedLock lock(_lock);
    Destroy();
    _resource.reset(resource, destroyFunction);
}

void ResourceFileNode::UpdateMemoryUse(int64_t currentBytes) noexcept
{
    autil::ScopedLock lock(_lock);
    if (_storageMetrics != nullptr) {
        if (currentBytes > _memoryUse) {
            _storageMetrics->IncreaseFileLength(GetMetricGroup(), FSFT_RESOURCE, currentBytes - _memoryUse);
        } else {
            _storageMetrics->DecreaseFileLength(GetMetricGroup(), FSFT_RESOURCE, _memoryUse - currentBytes);
        }
    }
    _memoryUse = currentBytes;
    SyncQuotaControl(false);
    if (_fileLenReporter) {
        _fileLenReporter->Record(currentBytes);
    }
}

FSFileType ResourceFileNode::GetType() const noexcept { return FSFT_RESOURCE; }

size_t ResourceFileNode::GetLength() const noexcept
{
    autil::ScopedLock lock(_lock);
    return _memoryUse > 0 ? _memoryUse : 0;
}

void* ResourceFileNode::GetBaseAddress() const noexcept
{
    AUTIL_LOG(ERROR, "resource file[%s] does not support get base address", _logicalPath.c_str());
    assert(false);
    return nullptr;
}
FSResult<size_t> ResourceFileNode::Read(void* buffer, size_t length, size_t offset, ReadOption option) noexcept
{
    AUTIL_LOG(ERROR, "resource file[%s] does not support read", _logicalPath.c_str());
    return {FSEC_NOTSUP, 0};
}
util::ByteSliceList* ResourceFileNode::ReadToByteSliceList(size_t length, size_t offset, ReadOption option) noexcept
{
    AUTIL_LOG(ERROR, "resource file[%s] does not support read", _logicalPath.c_str());
    assert(false);
    return nullptr;
}
FSResult<size_t> ResourceFileNode::Write(const void* buffer, size_t length) noexcept
{
    AUTIL_LOG(ERROR, "resource file[%s] does not support write", _logicalPath.c_str());
    assert(false);
    return {FSEC_NOTSUP, 0};
}
FSResult<void> ResourceFileNode::Close() noexcept { return FSEC_OK; }
bool ResourceFileNode::ReadOnly() const noexcept { return false; }

ErrorCode ResourceFileNode::DoOpen(const std::string& path, FSOpenType openType, int64_t fileLength) noexcept
{
    assert(openType == FSOT_RESOURCE);
    return FSEC_OK;
}
ErrorCode ResourceFileNode::DoOpen(const PackageOpenMeta& packageOpenMeta, FSOpenType openType) noexcept
{
    AUTIL_LOG(ERROR, "resource file[%s] does not support package", _logicalPath.c_str());
    return FSEC_NOTSUP;
}

void ResourceFileNode::SyncQuotaControl(bool forceSync) noexcept
{
    assert(_memController);
    size_t quota = _memController->GetUsedQuota();
    if (quota == _memoryUse) {
        return;
    }

    bool needSyncQuota = forceSync || quota == 0 || _memoryUse >= (quota + TRIGGER_UPDATE_MEMROY_DELTA_SIZE) ||
                         quota >= (_memoryUse + TRIGGER_UPDATE_MEMROY_DELTA_SIZE);
    if (!needSyncQuota) {
        // optimize, avoid call Allocate to much times
        return;
    }

    if (quota > _memoryUse) {
        size_t freeSize = quota - _memoryUse;
        _memController->Free(freeSize);
    } else {
        size_t expandSize = _memoryUse - quota;
        _memController->Allocate(expandSize);
    }
}

}} // namespace indexlib::file_system
