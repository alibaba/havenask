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
#include "master/BufferedSlotAllocator.h"
#include "master/BufferedHippoAdapter.h"
#include "master/SlotUtil.h"

BEGIN_CARBON_NAMESPACE(master);

namespace allocator {

using namespace hippo;
using namespace std;
using namespace autil;
using namespace autil::legacy;

#define SLAVE(slotId) (slotId.slaveAddress)

int Allocator::scoreSlotBasic(const SlotInfo& slot) {
    int score = 0;
    if (!SlotUtil::isSlotUnRecoverable(slot)) {
        score += 1;
    }
    if (SlotUtil::isPackageInstalled(slot)) {
        score += 10;
    }
    if (SlotUtil::isAllProcessRunning(slot)) {
        score += 100;
    }
    return score;
}

void Allocator::sortBasic(SlotInfoVect& slots) {
    std::sort(slots.begin(), slots.end(), [this] (const SlotInfo& s1, const SlotInfo& s2) {
            return this->scoreSlotBasic(s1) < this->scoreSlotBasic(s2);
        });
}

void Allocator::alloc(SlotInfoMap* dst, VirtualTag* tag, int32_t count) {
    dst->clear();
    SlotInfoVect bufferSlots;
    _buffer->getAllSlotInfos(bufferSlots);
    if (bufferSlots.empty()) return ;
    sortBasic(bufferSlots);
    while ((int32_t) dst->size() < count && !bufferSlots.empty()) {
        auto slot = bufferSlots.back();
        auto id = slot.slotId;
        dst->insert(std::make_pair(id, slot));
        bufferSlots.pop_back();
    }
}

ScatterAllocator::ScatterAllocator(SlotsBuffer* buffer) : Allocator(buffer) {
}

void ScatterAllocator::alloc(SlotInfoMap* dst, VirtualTag* tag, int32_t count) {
    dst->clear();
    SlotInfoVect bufferSlots;
    _buffer->getAllSlotInfos(bufferSlots);
    if (bufferSlots.empty()) return ;
    SlaveNodeCountMap countMap = buildNodeCountMap(tag);

    do {
        std::sort(bufferSlots.begin(), bufferSlots.end(), [&countMap, this] (const SlotInfo& s1, const SlotInfo& s2) {
                return this->scoreSlot(countMap, s1) < this->scoreSlot(countMap, s2);
            });
        auto slot = bufferSlots.back();
        auto id = slot.slotId;
        dst->insert(std::make_pair(id, slot));
        bufferSlots.pop_back();
        countMap[SLAVE(id)] = countMap[SLAVE(id)] + 1;
    } while (count > (int32_t) dst->size() && !bufferSlots.empty());
}

ScatterAllocator::SlaveNodeCountMap ScatterAllocator::buildNodeCountMap(const VirtualTag* tag) {
    SlaveNodeCountMap countMap;
    for (const auto& it : tag->slots) {
        countMap[SLAVE(it)] = countMap[SLAVE(it)] + 1;
    }
    return countMap;
}

int ScatterAllocator::scoreSlot(const SlaveNodeCountMap& countMap, const SlotInfo& slot) {
    int score = 100 * scoreSlotBasic(slot);
    std::string slave = SLAVE(slot.slotId);
    auto it = countMap.find(slave);
    return it == countMap.end() ? score : score - it->second;
}

}
END_CARBON_NAMESPACE(master);
