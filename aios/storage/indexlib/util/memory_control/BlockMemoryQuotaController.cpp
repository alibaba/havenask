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
#include "indexlib/util/memory_control/BlockMemoryQuotaController.h"

#include "indexlib/base/MemoryQuotaController.h"

namespace indexlib::util {

BlockMemoryQuotaController::BlockMemoryQuotaController(const PartitionMemoryQuotaControllerPtr& partitionMemController,
                                                       std::string name) noexcept
    : _name(name)
    , _leftQuota(0)
    , _usedQuota(0)
    , _partitionMemController(partitionMemController)
{
}
BlockMemoryQuotaController::BlockMemoryQuotaController(
    const std::shared_ptr<indexlibv2::MemoryQuotaController>& parentController, std::string name) noexcept
    : _name(name)
    , _leftQuota(0)
    , _usedQuota(0)
    , _parentController(parentController)
{
}
BlockMemoryQuotaController::~BlockMemoryQuotaController() noexcept
{
    DoFree(_leftQuota.load());
    assert(_usedQuota.load() == 0);
}

void BlockMemoryQuotaController::Allocate(int64_t quota) noexcept
{
    assert(quota >= 0);
    _leftQuota.fetch_sub(quota);
    if (_leftQuota.load() >= 0) {
        return;
    }
    int64_t leftQuota = _leftQuota.load();
    int64_t targetQuota = 0;
    int64_t allocateSize = 0;
    do {
        allocateSize = CalculateAllocateSize(leftQuota);
        targetQuota = leftQuota + allocateSize;
    } while (!_leftQuota.compare_exchange_weak(leftQuota, targetQuota));
    DoAllocate(allocateSize);
}

void BlockMemoryQuotaController::ShrinkToFit() noexcept { Free(0); }

bool BlockMemoryQuotaController::Reserve(int64_t quota) noexcept
{
    assert(quota >= 0);
    if (quota > GetFreeQuota()) {
        return false;
    }
    int64_t reserveSize = (quota + BLOCK_SIZE - 1) & (~BLOCK_SIZE_MASK);
    if (_partitionMemController) {
        if (!_partitionMemController->TryAllocate(reserveSize)) {
            return false;
        }
    } else {
        if (!_parentController->TryAllocate(reserveSize).IsOK()) {
            return false;
        }
    }
    _leftQuota.fetch_add(reserveSize);
    _usedQuota.fetch_add(reserveSize);
    return true;
}

void BlockMemoryQuotaController::Free(int64_t quota) noexcept
{
    assert(quota >= 0);
    _leftQuota.fetch_add(quota);
    int64_t leftQuota = _leftQuota.load();
    int64_t freeSize = 0;
    int64_t targetQuota = 0;
    do {
        freeSize = CalulateFreeSize(leftQuota);
        targetQuota = leftQuota - freeSize;
    } while (!_leftQuota.compare_exchange_weak(leftQuota, targetQuota));
    DoFree(freeSize);
}

int64_t BlockMemoryQuotaController::GetUsedQuota() const noexcept { return _usedQuota.load(); }

int64_t BlockMemoryQuotaController::GetAllocatedQuota() const noexcept { return _usedQuota.load() - _leftQuota.load(); }

const std::string& BlockMemoryQuotaController::GetName() const noexcept { return _name; }

const PartitionMemoryQuotaControllerPtr& BlockMemoryQuotaController::GetPartitionMemoryQuotaController() const noexcept
{
    return _partitionMemController;
}

int64_t BlockMemoryQuotaController::GetFreeQuota() const noexcept
{
    return _partitionMemController ? _partitionMemController->GetFreeQuota() : _parentController->GetFreeQuota();
}

int64_t BlockMemoryQuotaController::CalculateAllocateSize(int64_t leftQuota) noexcept
{
    if (leftQuota >= 0) {
        return 0;
    }
    return (-leftQuota + BLOCK_SIZE - 1) & (~BLOCK_SIZE_MASK);
}

int64_t BlockMemoryQuotaController::CalulateFreeSize(int64_t leftQuota) noexcept
{
    if (leftQuota <= 0) {
        return 0;
    }
    return leftQuota & (~BLOCK_SIZE_MASK);
}

void BlockMemoryQuotaController::DoAllocate(int64_t quota) noexcept
{
    if (_partitionMemController) {
        _partitionMemController->Allocate(quota);
    } else {
        _parentController->Allocate(quota);
    }
    _usedQuota.fetch_add(quota);
}

void BlockMemoryQuotaController::DoFree(int64_t quota) noexcept
{
    if (_partitionMemController) {
        _partitionMemController->Free(quota);
    } else {
        _parentController->Free(quota);
    }
    _usedQuota.fetch_sub(quota);
}

} // namespace indexlib::util
