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
#include "indexlib/util/EpochBasedReclaimManager.h"

#include "autil/TimeUtility.h"

namespace indexlib::util {
AUTIL_LOG_SETUP(indexlib.util, EpochBasedReclaimManager);

EpochBasedReclaimManager::EpochBasedReclaimManager(size_t reclaimThreshold, int64_t reclaimInterval)
    : _globalEpoch(1)
    , _threadData(new autil::ThreadLocalPtr(&DestroyThreadData))
    , _retiredItem(0)
    , _reclaimThreshold(reclaimThreshold)
    , _reclaimInterval(reclaimInterval)
    , _lastReclaimTime(autil::TimeUtility::currentTime())
{
}

EpochBasedReclaimManager::~EpochBasedReclaimManager() { Clear(); }

void EpochBasedReclaimManager::Clear()
{
    for (auto item : _retireList) {
        if (item.addr) {
            Free(item.addr);
        }
    }
    _retireList.clear();
}

void EpochBasedReclaimManager::Retire(void* addr)
{
    _retireList.push_back({addr, _globalEpoch});
    _retiredItem.store(_retireList.size(), std::memory_order_relaxed);

    if (_retireList.size() >= _reclaimThreshold) {
        TryReclaim();
    }
}

void EpochBasedReclaimManager::IncreaseEpoch() { _globalEpoch++; }
void EpochBasedReclaimManager::TryReclaim()
{
    if (_reclaimInterval > 0) {
        int64_t currentTime = autil::TimeUtility::currentTime();
        if (currentTime - _lastReclaimTime < _reclaimInterval) {
            return;
        }
        _lastReclaimTime = currentTime;
    }
    ThreadData data;
    int64_t globalEpoch = _globalEpoch.load();
    static constexpr int64_t EPOCH_MAX = std::numeric_limits<int64_t>::max();
    data.epoch = EPOCH_MAX;
    data.active = 0;
    _threadData->Fold(
        [](void* entryPtr, void* res) {
            auto threadData = reinterpret_cast<ThreadData*>(entryPtr);
            auto resData = reinterpret_cast<ThreadData*>(res);
            auto active = threadData->active.load(std::memory_order_acquire);
            auto epoch = threadData->epoch.load(std::memory_order_acquire);
            if (active != 0) {
                if (epoch < resData->epoch.load(std::memory_order_relaxed)) {
                    resData->epoch.store(epoch, std::memory_order_relaxed);
                    resData->active.store(true, std::memory_order_relaxed);
                }
            }
        },
        &data);
    int64_t maxReclaimEpoch = 0;
    if (data.active.load(std::memory_order_relaxed) == 0) {
        // no reader holds valid thread data, clear all legacy epoch
        maxReclaimEpoch = globalEpoch - 1;
    } else {
        // has active thread
        maxReclaimEpoch = data.epoch.load(std::memory_order_relaxed) - 1;
    }
    if (maxReclaimEpoch > 0) {
        while (!_retireList.empty()) {
            auto item = _retireList.front();
            if (item.epoch <= maxReclaimEpoch) {
                Free(item.addr);
                _retireList.pop_front();
            } else {
                break;
            }
        }
    }
    // IE_LOG(WARN, "fold results: epoch[%lu], active[%d], global epoch[%lu]", data.epoch, data.active,
    // _globalEpoch.load());
    _retiredItem.store(_retireList.size(), std::memory_order_relaxed);
}

EpochBasedReclaimManager::ThreadData* EpochBasedReclaimManager::GetThreadData()
{
    auto threadData = reinterpret_cast<ThreadData*>(_threadData->Get());
    if (threadData == nullptr) {
        threadData = new ThreadData();
        threadData->epoch = _globalEpoch.load();
        _threadData->Reset(threadData);
    }
    return threadData;
}

} // namespace indexlib::util
