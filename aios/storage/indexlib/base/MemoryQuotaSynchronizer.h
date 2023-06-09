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
#include <mutex>

#include "indexlib/base/MemoryQuotaController.h"

namespace indexlibv2 {

class MemoryQuotaSynchronizer
{
public:
    MemoryQuotaSynchronizer(const std::shared_ptr<MemoryQuotaController>& buildMemoryQuotaController)
        : _lastMemoryQuota(0)
        , _buildMemoryQuotaController(buildMemoryQuotaController)
    {
    }
    ~MemoryQuotaSynchronizer() = default;

public:
    int64_t GetFreeQuota() const;
    void SyncMemoryQuota(int64_t quota);
    int64_t GetTotalQuota() const;

private:
    mutable std::mutex _mutex;
    int64_t _lastMemoryQuota;
    std::shared_ptr<MemoryQuotaController> _buildMemoryQuotaController;
};

} // namespace indexlibv2
