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
#include <atomic>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <utility>

#include "indexlib/base/Status.h"

namespace indexlibv2 {

// TODO(hanyao): add quota limit to leaf nodes
class MemoryQuotaController
{
private:
    static constexpr int64_t BLOCK_SIZE = 4 * 1024 * 1024;
    static constexpr int64_t BLOCK_MASK = BLOCK_SIZE - 1;

public:
    MemoryQuotaController(std::string name, int64_t totalQuota) noexcept
        : _name(std::move(name))
        , _rootQuota(totalQuota)
        , _localFreeQuota(totalQuota)
        , _reservedParentQuota(0)
    {
    }

    MemoryQuotaController(std::string name, std::shared_ptr<MemoryQuotaController> parentController) noexcept
        : MemoryQuotaController(std::move(name), /*rootQuota*/ -1)
    {
        _parentController = std::move(parentController);
        _localFreeQuota = 0;
    }

    // for legacy use, will be deprecated in the future
    MemoryQuotaController(int64_t totalQuota) noexcept : MemoryQuotaController(/*name*/ "", totalQuota) {}
    ~MemoryQuotaController() noexcept { Free(GetAllocatedQuota()); }

public:
    void Allocate(int64_t quota) noexcept;
    // @return OK, NoMem
    Status TryAllocate(int64_t quota) noexcept;
    // @return OK, NoMem
    Status Reserve(int64_t quota) noexcept;
    void ShrinkToFit() noexcept;
    void Free(int64_t quota) noexcept;
    int64_t GetAllocatedQuota() const noexcept
    {
        return _parentController ? (_reservedParentQuota - _localFreeQuota) : (_rootQuota - _localFreeQuota);
    }
    int64_t GetFreeQuota() const noexcept
    {
        return _parentController ? _parentController->GetFreeQuota() : _localFreeQuota.load();
    }
    int64_t GetLocalFreeQuota() const noexcept { return _localFreeQuota.load(); }
    int64_t GetTotalQuota() const noexcept
    {
        return _parentController ? _parentController->GetTotalQuota() : _rootQuota;
    }

    std::string_view GetName() const noexcept { return _name; };

public:
    int64_t TEST_GetReservedParentQuota() const noexcept { return _reservedParentQuota.load(); }

private:
    void AllocateFromParent(int64_t quota) noexcept;
    void FreeToParent(int64_t quota) noexcept;
    int64_t BlockSize(int64_t quota) const noexcept;
    int64_t CalculateAllocateSize(int64_t leftQuota) const noexcept;
    int64_t CalulateFreeSize(int64_t leftQuota) const noexcept;

private:
    std::string _name;
    const int64_t _rootQuota;
    std::atomic<int64_t> _localFreeQuota;
    std::atomic<int64_t> _reservedParentQuota;
    std::shared_ptr<MemoryQuotaController> _parentController;
};

} // namespace indexlibv2
