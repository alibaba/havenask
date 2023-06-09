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

#include <memory>

#include "autil/Log.h"
#include "indexlib/util/memory_control/PartitionMemoryQuotaController.h"

namespace indexlibv2 {
class MemoryQuotaController;
}

namespace indexlib { namespace util {

class BlockMemoryQuotaController
{
public:
    BlockMemoryQuotaController(const PartitionMemoryQuotaControllerPtr& partitionMemController,
                               std::string name = "default_block_memory_quota_controll") noexcept;

    BlockMemoryQuotaController(const std::shared_ptr<indexlibv2::MemoryQuotaController>& parentController,
                               std::string name = "default_block_memory_quota_controll") noexcept;

    ~BlockMemoryQuotaController() noexcept;

public:
    void Allocate(int64_t quota) noexcept;
    void ShrinkToFit() noexcept;
    bool Reserve(int64_t quota) noexcept;
    void Free(int64_t quota) noexcept;
    int64_t GetUsedQuota() const noexcept;
    int64_t GetAllocatedQuota() const noexcept;
    const std::string& GetName() const noexcept;
    const PartitionMemoryQuotaControllerPtr& GetPartitionMemoryQuotaController() const noexcept;
    const std::shared_ptr<indexlibv2::MemoryQuotaController>& GetPartitionMemoryQuotaControllerV2() const noexcept
    {
        return _parentController;
    }
    int64_t GetFreeQuota() const noexcept;

public:
    static const int64_t BLOCK_SIZE = 4 * 1024 * 1024;
    static const int64_t BLOCK_SIZE_MASK = BLOCK_SIZE - 1;

private:
    int64_t CalculateAllocateSize(int64_t leftQuota) noexcept;
    int64_t CalulateFreeSize(int64_t leftQuota) noexcept;
    void DoAllocate(int64_t quota) noexcept;
    void DoFree(int64_t quota) noexcept;

private:
    std::string _name;
    std::atomic<int64_t> _leftQuota;
    // quota allocated from _partitionMemController, including quota in _leftQuota
    std::atomic<int64_t> _usedQuota;
    PartitionMemoryQuotaControllerPtr _partitionMemController;
    std::shared_ptr<indexlibv2::MemoryQuotaController> _parentController;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<BlockMemoryQuotaController> BlockMemoryQuotaControllerPtr;
}} // namespace indexlib::util
