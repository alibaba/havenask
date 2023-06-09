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
#include <deque>
#include <memory>

// #include "autil/CommonMacros.h"
#include "autil/Log.h"
#include "autil/ThreadLocal.h"

namespace indexlib::util {

// TODO(panghai.hj): Add support for queries/readers using e.g. coroutine or fibers. This currently only supports
// one/multiple queries/reader per thread.
// As discussed, it might be more appropriate to bookkeep memory at partition or index level.
// e.g. For each query that acceses partition reader, assign epoch to each query(this requires significant code
// refactoring work), and partition will hold node manager. Or, for each index(posting table), assign an epoch when
// iterator is created, node manger will be held at posting table level.(This might affect query performance, but might
// worth a try).
class EpochBasedReclaimManager
{
private:
    struct ThreadData {
        std::atomic<int64_t> epoch;
        std::atomic<int64_t> active;

        ThreadData() : epoch(0), active(0) {}
    };
    struct RetireItem {
        void* addr = nullptr;
        int64_t epoch = 0;
    };

public:
    class CriticalGuard
    {
    public:
        CriticalGuard() : _threadData(nullptr) {}
        CriticalGuard(const CriticalGuard&) = delete;
        CriticalGuard& operator=(const CriticalGuard&) = delete;
        CriticalGuard(CriticalGuard&& other) : _threadData(std::exchange(other._threadData, nullptr)) {}
        CriticalGuard& operator=(CriticalGuard&& other)
        {
            if (this != &other) {
                if (_threadData) {
                    LeaveCritical(_threadData);
                }
                _threadData = std::exchange(other._threadData, nullptr);
            }
            return *this;
        }

    public:
        CriticalGuard(EpochBasedReclaimManager* manager) : _threadData(nullptr)
        {
            _threadData = manager->GetThreadData();
            assert(_threadData);

            if (_threadData->active.fetch_add(1, std::memory_order_release) == 0) {
                _threadData->epoch.store(manager->GetGlobalEpoch(), std::memory_order_release);
            }
        }
        ~CriticalGuard()
        {
            if (_threadData) {
                LeaveCritical(_threadData);
            }
        }

    private:
        void LeaveCritical(ThreadData* threadData) { threadData->active.fetch_sub(1, std::memory_order_release); }

    private:
        ThreadData* _threadData;
    };

public:
    EpochBasedReclaimManager(size_t reclaimThreshold, int64_t reclaimInterval);
    virtual ~EpochBasedReclaimManager();

    EpochBasedReclaimManager(const EpochBasedReclaimManager&) = delete;
    EpochBasedReclaimManager(EpochBasedReclaimManager&&) = delete;

public:
    void Clear();
    void Retire(void* addr);

    AUTIL_NODISCARD CriticalGuard CriticalScope() { return CriticalGuard(this); }
    void IncreaseEpoch();
    int64_t GetGlobalEpoch() const { return _globalEpoch.load(std::memory_order_acquire); }
    size_t RetiredItem() const { return _retiredItem.load(std::memory_order_relaxed); }

protected:
    virtual void Free(void* addr) = 0;

private:
    ThreadData* GetThreadData();
    void TryReclaim();

private:
    static void DestroyThreadData(void* ptr)
    {
        assert(ptr);
        auto threadData = reinterpret_cast<ThreadData*>(ptr);
        delete threadData;
    }

private:
    std::atomic<int64_t> _globalEpoch;
    std::unique_ptr<autil::ThreadLocalPtr> _threadData;
    std::deque<RetireItem> _retireList;
    std::atomic<size_t> _retiredItem;

    const size_t _reclaimThreshold;
    const int64_t _reclaimInterval;
    int64_t _lastReclaimTime;

private:
    friend class EpochBasedReclaimManagerTest;
    AUTIL_LOG_DECLARE();
};
} // namespace indexlib::util
