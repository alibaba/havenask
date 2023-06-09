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
#include "indexlib/base/MemoryQuotaSynchronizer.h"

namespace indexlibv2 {

int64_t MemoryQuotaSynchronizer::GetFreeQuota() const
{
    std::lock_guard<std::mutex> guard(_mutex);
    return _buildMemoryQuotaController->GetFreeQuota();
}

void MemoryQuotaSynchronizer::SyncMemoryQuota(int64_t quota)
{
    std::lock_guard<std::mutex> guard(_mutex);
    if (_lastMemoryQuota == quota) {
        return;
    }

    if (quota > _lastMemoryQuota) {
        _buildMemoryQuotaController->Allocate(quota - _lastMemoryQuota);
    } else {
        _buildMemoryQuotaController->Free(_lastMemoryQuota - quota);
    }
    _lastMemoryQuota = quota;
    return;
}

int64_t MemoryQuotaSynchronizer::GetTotalQuota() const { return _buildMemoryQuotaController->GetTotalQuota(); }

} // namespace indexlibv2
