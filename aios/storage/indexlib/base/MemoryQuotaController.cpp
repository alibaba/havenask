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
#include "indexlib/base/MemoryQuotaController.h"

#include <cassert>

namespace indexlibv2 {

void MemoryQuotaController::Allocate(int64_t quota) noexcept
{
    assert(quota >= 0);
    auto preValue = _localFreeQuota.fetch_sub(quota);
    if (!_parentController or preValue >= quota) {
        return;
    }
    int64_t leftQuota = _localFreeQuota.load();
    int64_t targetQuota = 0;
    int64_t allocateSize = 0;
    do {
        allocateSize = CalculateAllocateSize(leftQuota);
        targetQuota = leftQuota + allocateSize;
    } while (!_localFreeQuota.compare_exchange_weak(leftQuota, targetQuota));
    AllocateFromParent(allocateSize);
}

Status MemoryQuotaController::TryAllocate(int64_t quota) noexcept
{
    assert(quota >= 0);
    if (_parentController) {
        auto localFreeQuota = _localFreeQuota.load();
        int64_t targetLocalFreeQuota = 0;
        int64_t allocateSize = 0;
        do {
            if (localFreeQuota >= quota) {
                targetLocalFreeQuota = localFreeQuota - quota;
                allocateSize = 0;
            } else if (localFreeQuota >= 0) {
                targetLocalFreeQuota = 0;
                allocateSize = quota - localFreeQuota;
            } else {
                targetLocalFreeQuota = localFreeQuota;
                allocateSize = quota;
            }
        } while (!_localFreeQuota.compare_exchange_weak(localFreeQuota, targetLocalFreeQuota));
        allocateSize = BlockSize(allocateSize);
        if (!_parentController->TryAllocate(allocateSize).IsOK()) {
            _localFreeQuota.fetch_add(localFreeQuota - targetLocalFreeQuota); // return local allocated memory
            return Status::NoMem("Try Allocate Quota Failed", "parent try allocate failed");
        }
        if (allocateSize != 0) {
            _localFreeQuota.fetch_add(allocateSize - (quota - localFreeQuota + targetLocalFreeQuota));
        }
        _reservedParentQuota.fetch_add(allocateSize);
        return Status::OK();
    } else {
        int64_t leftQuota = _localFreeQuota.load();
        int64_t targetLeftQuota = 0;
        do {
            if (leftQuota < quota) {
                return Status::NoMem("TryAllocate Quota Failed");
            }
            targetLeftQuota = leftQuota - quota;
        } while (!_localFreeQuota.compare_exchange_weak(leftQuota, targetLeftQuota));
        return Status::OK();
    }
}

int64_t MemoryQuotaController::BlockSize(int64_t quota) const noexcept
{
    return (quota + BLOCK_SIZE - 1) & (~BLOCK_MASK);
}

Status MemoryQuotaController::Reserve(int64_t quota) noexcept
{
    assert(quota >= 0);
    if (_parentController) {
        int64_t allocateSize = BlockSize(quota);
        auto st = _parentController->TryAllocate(allocateSize);
        if (st.IsOK()) {
            _reservedParentQuota.fetch_add(allocateSize);
            _localFreeQuota.fetch_add(allocateSize);
        }
        return st;
    }
    assert(false);
    return Status::NoMem("Reserve Not Supportted on root controller");
}

void MemoryQuotaController::Free(int64_t quota) noexcept
{
    assert(quota >= 0);
    auto preValue = _localFreeQuota.fetch_add(quota);
    if (!_parentController or preValue + quota < BLOCK_SIZE) {
        return;
    }
    int64_t leftQuota = _localFreeQuota.load();
    int64_t freeSize = 0;
    int64_t targetQuota = 0;
    do {
        freeSize = CalulateFreeSize(leftQuota);
        targetQuota = leftQuota - freeSize;
    } while (!_localFreeQuota.compare_exchange_weak(leftQuota, targetQuota));
    FreeToParent(freeSize);
}

void MemoryQuotaController::ShrinkToFit() noexcept { Free(0); }

void MemoryQuotaController::AllocateFromParent(int64_t quota) noexcept
{
    _parentController->Allocate(quota);
    _reservedParentQuota.fetch_add(quota);
}

void MemoryQuotaController::FreeToParent(int64_t quota) noexcept
{
    _parentController->Free(quota);
    _reservedParentQuota.fetch_sub(quota);
}

int64_t MemoryQuotaController::CalculateAllocateSize(int64_t leftQuota) const noexcept
{
    if (leftQuota >= 0) {
        return 0;
    }
    return (-leftQuota + BLOCK_SIZE - 1) & (~BLOCK_MASK);
}

int64_t MemoryQuotaController::CalulateFreeSize(int64_t leftQuota) const noexcept
{
    if (leftQuota <= 0) {
        return 0;
    }
    return leftQuota & (~BLOCK_MASK);
}

} // namespace indexlibv2
