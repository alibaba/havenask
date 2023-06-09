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

#include "autil/Lock.h"
#include "autil/Log.h"

namespace indexlib { namespace util {

// 一个内存控制器，quota 不足时 Allocate() 会不消耗 CPU 的 wait
class WaitMemoryQuotaController
{
public:
    WaitMemoryQuotaController(int64_t totalQuota) : _leftQuota(totalQuota), _totalQuota(totalQuota) {}
    ~WaitMemoryQuotaController() = default;

    WaitMemoryQuotaController(const WaitMemoryQuotaController&) = delete;
    WaitMemoryQuotaController& operator=(const WaitMemoryQuotaController&) = delete;
    WaitMemoryQuotaController(WaitMemoryQuotaController&&) = delete;
    WaitMemoryQuotaController& operator=(WaitMemoryQuotaController&&) = delete;

public:
    bool Allocate(int64_t quota)
    {
        assert(quota >= 0);
        if (unlikely(quota > _totalQuota)) {
            return false;
        }
        do {
            int64_t leftQuota = _leftQuota.load();
            if (leftQuota >= quota) {
                if (_leftQuota.compare_exchange_strong(leftQuota, leftQuota - quota)) {
                    return true;
                }
            }
        } while (0 == _notifier.waitNotification());
        assert(false);
        return false;
    }

    void Free(int64_t quota)
    {
        assert(quota >= 0);
        _leftQuota.fetch_add(quota);
        _notifier.notify();
    }

    void AddQuota(int64_t quota)
    {
        if (unlikely(quota <= 0)) {
            return;
        }
        _totalQuota += quota;
        Free(quota);
    }

    int64_t GetFreeQuota() const { return _leftQuota.load(); }
    int64_t GetTotalQuota() const { return _totalQuota; }
    int64_t GetUsedQuota() const { return _totalQuota - _leftQuota.load(); }

private:
    std::atomic<int64_t> _leftQuota;
    int64_t _totalQuota = 0;
    autil::Notifier _notifier;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<WaitMemoryQuotaController> WaitMemoryQuotaControllerPtr;

}} // namespace indexlib::util
