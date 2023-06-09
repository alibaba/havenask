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
#include <mutex>

#include "autil/Log.h"
#include "indexlib/framework/mem_reclaimer/IIndexMemoryReclaimer.h"
#include "indexlib/framework/mem_reclaimer/MemReclaimerMetrics.h"

namespace indexlibv2::framework {

class EpochBasedMemReclaimer : public IIndexMemoryReclaimer
{
public:
    EpochBasedMemReclaimer(const std::shared_ptr<MemReclaimerMetrics>& memReclaimerMetrics);
    EpochBasedMemReclaimer(size_t recliamFreq, const std::shared_ptr<MemReclaimerMetrics>& memReclaimerMetrics);
    ~EpochBasedMemReclaimer();

    struct RetireItem {
        void* addr = nullptr;
        int64_t epoch = 0;
        std::function<void(void*)> deAllocator;
    };

    struct EpochItem {
        EpochItem(int64_t iEpoch, int64_t iUseCount) : epoch(iEpoch), useCount(iUseCount) {}
        std::atomic<int64_t> epoch;
        std::atomic<int64_t> useCount;
    };

public:
    void Retire(void* addr, std::function<void(void*)> deAllocator) override;
    void TryReclaim() override;

public:
    void Clear();
    void IncreaseEpoch();
    EpochItem* CriticalGuard();
    void LeaveCritical(EpochItem* epochItem);

private:
    void DoReclaim();

private:
    std::deque<RetireItem> _retireList;
    std::deque<EpochItem> _epochItemList;

    std::atomic<int64_t> _globalEpoch;
    mutable std::mutex _epochMutex;
    mutable std::mutex _retireMutex;

    size_t _reclaimFreq;
    size_t _tryReclaimCounter;
    std::shared_ptr<MemReclaimerMetrics> _memReclaimerMetrics;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::framework
