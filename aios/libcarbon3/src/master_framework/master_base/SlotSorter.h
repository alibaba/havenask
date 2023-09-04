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
#ifndef MASTER_FRAMEWORK_SLOTSORTER_H
#define MASTER_FRAMEWORK_SLOTSORTER_H

#include "master_framework/common.h"
#include "hippo/DriverCommon.h"
#include "common/Log.h"

BEGIN_MASTER_FRAMEWORK_NAMESPACE(master_base);

struct SlotWrapper {
    int64_t score;
    const hippo::SlotInfo *slotInfo;

    SlotWrapper(const hippo::SlotInfo *slotInfo, int32_t score) {
        this->score = score;
        this->slotInfo = slotInfo;
    }

    bool operator < (const SlotWrapper &rhs) const {
        return score < rhs.score;
    }
};

class SlotSorter
{
public:
    SlotSorter();
    ~SlotSorter();
private:
    SlotSorter(const SlotSorter &);
    SlotSorter& operator=(const SlotSorter &);
public:
    void sort(std::vector<const hippo::SlotInfo*> &slotInfos);
    
private:
    int64_t score(const hippo::SlotInfo *slotInfo);
private:
    MASTER_FRAMEWORK_LOG_DECLARE();
};

MASTER_FRAMEWORK_TYPEDEF_PTR(SlotSorter);

END_MASTER_FRAMEWORK_NAMESPACE(master_base);

#endif //MASTER_FRAMEWORK_SLOTSORTER_H
