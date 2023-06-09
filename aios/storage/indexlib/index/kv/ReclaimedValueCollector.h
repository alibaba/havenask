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
#include <unordered_map>

#include "autil/Lock.h"
#include "indexlib/framework/mem_reclaimer/IIndexMemoryReclaimer.h"

namespace indexlibv2::index {

template <typename ValueOffsetType>
class ReclaimedValueCollector
{
    using Length2ValueOffsetsMap = std::unordered_map<size_t, std::vector<ValueOffsetType>>;

private:
    ReclaimedValueCollector(indexlibv2::framework::IIndexMemoryReclaimer* memReclaimer) : _memReclaimer(memReclaimer)
    {
        assert(_memReclaimer != nullptr);
        _lock = std::make_shared<autil::ThreadMutex>();
        _len2OffsetsMap = std::make_shared<Length2ValueOffsetsMap>();
    }

public:
    ~ReclaimedValueCollector() {}

public:
    static std::shared_ptr<ReclaimedValueCollector<ValueOffsetType>>
    Create(indexlibv2::framework::IIndexMemoryReclaimer* memReclaimer)
    {
        if (memReclaimer == nullptr) {
            return std::shared_ptr<ReclaimedValueCollector<ValueOffsetType>>();
        }
        return std::shared_ptr<ReclaimedValueCollector<ValueOffsetType>>(
            new ReclaimedValueCollector<ValueOffsetType>(memReclaimer));
    }

    void Collect(size_t length, ValueOffsetType offset)
    {
        auto lock = _lock;
        auto len2OffsetsMap = _len2OffsetsMap;
        auto deAllocator = [lock, len2OffsetsMap, length, offset](void*) mutable {
            DoCollect(lock, len2OffsetsMap, length, offset);
        };
        _memReclaimer->Retire(nullptr, deAllocator);
    }

    bool PopLengthEqualTo(size_t length, ValueOffsetType& offset)
    {
        autil::ScopedLock _(*_lock);
        auto iterator = _len2OffsetsMap->find(length);
        if (iterator == _len2OffsetsMap->end()) {
            return false;
        }
        auto& reclaimedOffsets = iterator->second;
        if (reclaimedOffsets.empty()) {
            return false;
        }
        offset = reclaimedOffsets.back();
        reclaimedOffsets.pop_back();
        return true;
    }

private:
    static void DoCollect(std::shared_ptr<autil::ThreadMutex>& lock,
                          std::shared_ptr<Length2ValueOffsetsMap>& len2OffsetsMap, size_t length,
                          ValueOffsetType offset)
    {
        autil::ScopedLock _(*lock);
        (*len2OffsetsMap)[length].push_back(offset);
    }

private:
    indexlibv2::framework::IIndexMemoryReclaimer* _memReclaimer;
    std::shared_ptr<autil::ThreadMutex> _lock;
    std::shared_ptr<Length2ValueOffsetsMap> _len2OffsetsMap;
};

} // namespace indexlibv2::index
