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

#include <cassert>
#include <memory>

#include "autil/Log.h"
#include "indexlib/util/memory_control/MemoryQuotaController.h"
#include "indexlib/util/memory_control/QuotaControl.h"

namespace indexlib { namespace util {

class PartitionMemoryQuotaController
{
public:
    PartitionMemoryQuotaController(const MemoryQuotaControllerPtr& memController,
                                   const std::string& name = "default_partition") noexcept
        : _name(name)
        , _totalMemoryController(memController)
        , _usedQuota(0)
    {
    }
    ~PartitionMemoryQuotaController() noexcept
    {
        assert(_usedQuota.load() == 0);
        _totalMemoryController->Free(_usedQuota.load());
    }

public:
    void Allocate(int64_t quota) noexcept
    {
        assert(quota >= 0);
        if (quota <= 0) {
            return;
        }
        _totalMemoryController->Allocate(quota);
        _usedQuota.fetch_add(quota);
    }

    bool TryAllocate(int64_t quota) noexcept
    {
        if (_totalMemoryController->TryAllocate(quota).IsOK()) {
            _usedQuota.fetch_add(quota);
            return true;
        }
        return false;
    }

    void Free(int64_t quota) noexcept
    {
        assert(quota >= 0);
        if (quota <= 0) {
            return;
        }
        _totalMemoryController->Free(quota);
        _usedQuota.fetch_sub(quota);
    }

    int64_t GetFreeQuota() const noexcept { return _totalMemoryController->GetFreeQuota(); }

    int64_t GetUsedQuota() const noexcept { return _usedQuota.load(); }

    int64_t GetTotalQuotaLimit() const noexcept { return _totalMemoryController->GetTotalQuota(); }

    const std::string& GetName() const noexcept { return _name; }

public:
    // TODO: remove this
    const QuotaControlPtr& GetBuildMemoryQuotaControler() const { return _buildMemoryQuotaControler; }
    void SetBuildMemoryQuotaControler(const QuotaControlPtr& buildMemoryQuotaControler) noexcept
    {
        _buildMemoryQuotaControler = buildMemoryQuotaControler;
    }

private:
    std::string _name;
    MemoryQuotaControllerPtr _totalMemoryController;
    std::atomic<int64_t> _usedQuota;
    QuotaControlPtr _buildMemoryQuotaControler;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<PartitionMemoryQuotaController> PartitionMemoryQuotaControllerPtr;
}} // namespace indexlib::util
