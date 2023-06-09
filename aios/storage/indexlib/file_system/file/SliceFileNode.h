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
#include <memory>
#include <stddef.h>
#include <stdint.h>
#include <string>
#include <vector>

#include "autil/Log.h"
#include "indexlib/file_system/FileSystemDefine.h"
#include "indexlib/file_system/StorageMetrics.h"
#include "indexlib/file_system/file/FileNode.h"
#include "indexlib/util/byte_slice_list/ByteSliceList.h"
#include "indexlib/util/memory_control/BlockMemoryQuotaController.h"
#include "indexlib/util/slice_array/BytesAlignedSliceArray.h"

namespace indexlib { namespace file_system {
class PackageOpenMeta;
struct ReadOption;
}} // namespace indexlib::file_system
// ATTENTION: thread-safe only when
// totalSize < reservedSliceNum * sliceLen
// ATTENTION: user must ensure sliceLen >= MaxValueLength

namespace indexlib { namespace file_system {

class FSSliceMemoryMetrics : public util::SliceMemoryMetrics
{
public:
    FSSliceMemoryMetrics(FSMetricGroup metricGroup, StorageMetrics* storageMetrics) noexcept
        : _metricGroup(metricGroup)
        , _storageMetrics(storageMetrics)
    {
    }

    void Increase(size_t size) noexcept override
    {
        if (_storageMetrics) {
            _storageMetrics->IncreaseFileLength(_metricGroup, FSFT_SLICE, size);
        }
    }

    void Decrease(size_t size) noexcept override
    {
        if (_storageMetrics) {
            _storageMetrics->DecreaseFileLength(_metricGroup, FSFT_SLICE, size);
        }
    }

private:
    FSMetricGroup _metricGroup;
    StorageMetrics* _storageMetrics;
};

class SliceFileNode : public FileNode
{
public:
    typedef std::vector<uint8_t*> SlicePrtVec;

public:
    SliceFileNode(uint64_t sliceLen, int32_t sliceNum,
                  const util::BlockMemoryQuotaControllerPtr& memController) noexcept;
    ~SliceFileNode() noexcept;

public:
    FSFileType GetType() const noexcept override;
    size_t GetLength() const noexcept override;
    void* GetBaseAddress() const noexcept override;
    FSResult<size_t> Read(void* buffer, size_t length, size_t offset, ReadOption option) noexcept override;
    util::ByteSliceList* ReadToByteSliceList(size_t length, size_t offset, ReadOption option) noexcept override;
    FSResult<size_t> Write(const void* buffer, size_t length) noexcept override;
    FSResult<void> Close() noexcept override;
    bool ReadOnly() const noexcept override { return false; }

public:
    const util::BytesAlignedSliceArrayPtr& GetBytesAlignedSliceArray() const noexcept
    {
        return _bytesAlignedSliceArray;
    }

    void SetStorageMetrics(FSMetricGroup metricGroup, StorageMetrics* storageMetrics) noexcept
    {
        util::SliceMemoryMetricsPtr metrics(new FSSliceMemoryMetrics(metricGroup, storageMetrics));
        _bytesAlignedSliceArray->SetSliceMemoryMetrics(metrics);
    }

    size_t GetSliceFileLength() const noexcept { return _bytesAlignedSliceArray->GetTotalUsedBytes(); }

    void SetCleanCallback(std::function<void()>&& cleanFunc) noexcept { _cleanFunc = std::move(cleanFunc); }

private:
    ErrorCode DoOpen(const std::string& path, FSOpenType openType, int64_t fileLength) noexcept override;
    ErrorCode DoOpen(const PackageOpenMeta& packageOpenMeta, FSOpenType openType) noexcept override;

private:
    util::BytesAlignedSliceArrayPtr _bytesAlignedSliceArray;
    std::function<void()> _cleanFunc;
    int32_t _sliceNum;

private:
    AUTIL_LOG_DECLARE();
    friend class SliceFileNodeTest;
};

typedef std::shared_ptr<SliceFileNode> SliceFileNodePtr;
}} // namespace indexlib::file_system
