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
#include <memory>

#include "autil/Log.h"
#include "indexlib/util/memory_control/BlockMemoryQuotaController.h"

namespace indexlib { namespace util {

template <typename T>
class SimpleMemoryQuotaControllerType
{
public:
    SimpleMemoryQuotaControllerType(const BlockMemoryQuotaControllerPtr& memController) noexcept
        : _usedQuota(0)
        , _memController(memController)
    {
        assert(memController);
    }

    ~SimpleMemoryQuotaControllerType() noexcept { _memController->Free(_usedQuota); }

public:
    void Allocate(int64_t quota) noexcept
    {
        _usedQuota += quota;
        _memController->Allocate(quota);
    }

    void Free(int64_t quota) noexcept
    {
        _usedQuota -= quota;
        _memController->Free(quota);
    }

    int64_t GetFreeQuota() const noexcept { return _memController->GetFreeQuota(); }

    int64_t GetUsedQuota() const noexcept { return _usedQuota; }

    const BlockMemoryQuotaControllerPtr& GetBlockMemoryController() const noexcept { return _memController; }

private:
    T _usedQuota;
    BlockMemoryQuotaControllerPtr _memController;

private:
    AUTIL_LOG_DECLARE();
};

typedef SimpleMemoryQuotaControllerType<std::atomic<int64_t>> SimpleMemoryQuotaController;
typedef SimpleMemoryQuotaControllerType<int64_t> UnsafeSimpleMemoryQuotaController;

typedef std::shared_ptr<SimpleMemoryQuotaController> SimpleMemoryQuotaControllerPtr;
typedef std::shared_ptr<UnsafeSimpleMemoryQuotaController> UnsafeSimpleMemoryQuotaControllerPtr;
}} // namespace indexlib::util
