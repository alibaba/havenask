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
#include "indexlib/framework/mem_reclaimer/EpochBasedMemReclaimer.h"

namespace indexlibv2::framework {
AUTIL_LOG_SETUP(indexlib.framework, EpochBasedMemReclaimer);

static constexpr size_t DEFAULT_RECLAIM_FREQUENCY = 10;

EpochBasedMemReclaimer::EpochBasedMemReclaimer(const std::shared_ptr<MemReclaimerMetrics>& memReclaimerMetrics)
    : _globalRetireId(0)
    , _globalEpoch(0)
    , _reclaimFreq(DEFAULT_RECLAIM_FREQUENCY)
    , _tryReclaimCounter(0)
    , _memReclaimerMetrics(memReclaimerMetrics)
{
    IncreaseEpoch();
}

EpochBasedMemReclaimer::EpochBasedMemReclaimer(size_t recliamFreq,
                                               const std::shared_ptr<MemReclaimerMetrics>& memReclaimerMetrics)
    : _globalEpoch(0)
    , _reclaimFreq(recliamFreq)
    , _tryReclaimCounter(0)
    , _memReclaimerMetrics(memReclaimerMetrics)
{
    IncreaseEpoch();
}

EpochBasedMemReclaimer::~EpochBasedMemReclaimer() { Clear(); }

void EpochBasedMemReclaimer::Clear()
{
    for (auto item : _retireList) {
        if (item.addr) {
            item.deAllocator(item.addr);
        }
    }
    _retireList.clear();
}

int64_t EpochBasedMemReclaimer::Retire(void* addr, std::function<void(void*)> deAllocator)
{
    std::lock_guard<std::mutex> guard(_epochMutex);
    _retireList.push_back({_globalRetireId, addr, _globalEpoch, deAllocator});
    _globalRetireId++;
    if (_memReclaimerMetrics) {
        _memReclaimerMetrics->IncreasetotalReclaimEntriesValue(1);
    }
    return _globalRetireId - 1;
}

void EpochBasedMemReclaimer::DropRetireItem(int64_t retireItemId)
{
    std::lock_guard<std::mutex> guard(_reclaimMutex);
    std::lock_guard<std::mutex> guard1(_epochMutex);
    for (size_t i = 0; i < _retireList.size(); i++) {
        if (_retireList.at(i).retireId == retireItemId) {
            _retireList.erase(_retireList.begin() + i);
            return;
        }
    }
}

void EpochBasedMemReclaimer::TryReclaim()
{
    if (_reclaimFreq == 0) {
        return Reclaim();
    }

    _tryReclaimCounter = (_tryReclaimCounter + 1) % _reclaimFreq;
    if (_tryReclaimCounter == 0) {
        return Reclaim();
    }
    IncreaseEpoch();
}

EpochBasedMemReclaimer::EpochItem* EpochBasedMemReclaimer::CriticalGuard()
{
    std::lock_guard<std::mutex> guard(_epochMutex);
    auto& epochItem = _epochItemList.back();
    epochItem.useCount += 1;
    return &epochItem;
}

void EpochBasedMemReclaimer::LeaveCritical(EpochBasedMemReclaimer::EpochItem* epochItem)
{
    if (epochItem) {
        epochItem->useCount -= 1;
    }
}

void EpochBasedMemReclaimer::IncreaseEpoch()
{
    std::lock_guard<std::mutex> guard(_epochMutex);
    _globalEpoch++;
    _epochItemList.emplace_back(_globalEpoch.load(), 0);
}

void EpochBasedMemReclaimer::GetMaxMinEpochId(int64_t& maxEpochId, int64_t& minEpochId)
{
    std::lock_guard<std::mutex> guard(_epochMutex);
    maxEpochId = _globalEpoch;
    minEpochId = _globalEpoch;
    if (!_epochItemList.empty()) {
        minEpochId = _epochItemList.front().epoch;
    }
}

void EpochBasedMemReclaimer::Reclaim()
{
    int64_t maxReclaimEpoch = -1;
    {
        std::lock_guard<std::mutex> guard(_epochMutex);
        while (!_epochItemList.empty()) {
            auto& item = _epochItemList.front();
            if (item.useCount > 0) {
                // find active owner
                break;
            }
            maxReclaimEpoch = item.epoch.load(std::memory_order_relaxed) - 1;
            _epochItemList.pop_front();
        }
        _globalEpoch++;
        _epochItemList.emplace_back(_globalEpoch.load(), 0);
    }
    // free memory
    std::deque<RetireItem> freeItemList;
    std::lock_guard<std::mutex> guard(_reclaimMutex);
    {
        std::lock_guard<std::mutex> guard(_epochMutex);
        if (maxReclaimEpoch > 0) {
            while (!_retireList.empty()) {
                auto item = _retireList.front();
                if (item.epoch <= maxReclaimEpoch) {
                    freeItemList.emplace_back(item);
                    _retireList.pop_front();
                } else {
                    break;
                }
            }
        }
    }
    int64_t freeEntries = 0;
    for (auto& item : freeItemList) {
        item.deAllocator(item.addr);
        freeEntries++;
    }
    if (_memReclaimerMetrics) {
        _memReclaimerMetrics->IncreasetotalFreeEntriesValue(freeEntries);
    }
}

} // namespace indexlibv2::framework
