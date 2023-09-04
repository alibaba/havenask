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
#include "master_base/SlotSorter.h"
#include "common/Log.h"

using namespace std;
using namespace hippo;

BEGIN_MASTER_FRAMEWORK_NAMESPACE(master_base);

//MASTER_FRAMEWORK_LOG_SETUP(master_base, SlotSorter);

SlotSorter::SlotSorter() {
}

SlotSorter::~SlotSorter() {
}

int64_t SlotSorter::score(const SlotInfo *slotInfo) {
    return slotInfo->slotId.declareTime;
}

void SlotSorter::sort(vector<const SlotInfo*> &slotInfos) {
    vector<SlotWrapper> slots;
    slots.reserve(slotInfos.size());
    for (size_t i = 0; i < slotInfos.size(); i++) {
        slots.push_back(SlotWrapper(slotInfos[i], score(slotInfos[i])));
    }

    std::sort(slots.begin(), slots.end());

    slotInfos.clear();
    for (size_t i = 0; i < slots.size(); i++) {
        slotInfos.push_back(slots[i].slotInfo);
    }
}

END_MASTER_FRAMEWORK_NAMESPACE(master_base);

