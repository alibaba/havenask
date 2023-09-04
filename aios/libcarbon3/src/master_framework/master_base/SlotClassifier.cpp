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
#include "master_base/SlotClassifier.h"
#include "common/Log.h"

using namespace std;
using namespace hippo;

BEGIN_MASTER_FRAMEWORK_NAMESPACE(master_base);

MASTER_FRAMEWORK_LOG_SETUP(master_base, SlotClassifier);

SlotClassifier::SlotClassifier() {
}

SlotClassifier::~SlotClassifier() {
}

void SlotClassifier::classifySlots(
        const SlotInfos &slots,
        vector<const SlotInfo*> &onlineSlots,
        vector<const SlotInfo*> &needReleaseSlots,
        vector<const SlotInfo*> &needResetSlots,
        vector<const SlotInfo*> &needRestartProcSlots,
        bool reclaimingAutoRelease,
        bool longTimeNotRunningAutoRelease,
        const set<SlotId> &longTimeNotRunningSlots)
{
    for (size_t i = 0; i < slots.size(); i++) {
        const SlotInfo* slotInfo = &slots[i];
        
        MF_LOG(DEBUG, "slotinfo: %s, %s, %d, %d.",
               slotInfo->role.c_str(),
               slotInfo->slotId.slaveAddress.c_str(),
               slotInfo->slotId.id, slotInfo->reclaiming);
        if (slotInfo->reclaiming && reclaimingAutoRelease)
        {
            needReleaseSlots.push_back(slotInfo);
            continue;
        }

        if (longTimeNotRunningSlots.find(slotInfo->slotId) !=
            longTimeNotRunningSlots.end() && longTimeNotRunningAutoRelease)
        {
            needReleaseSlots.push_back(slotInfo);
            continue;
        }
        
        onlineSlots.push_back(slotInfo);
        
        if (slotInfo->packageStatus.status == PackageStatus::IS_FAILED) {
            needResetSlots.push_back(slotInfo); 
            continue;
        }

        if (hasFailedProcess(slotInfo)) {
            needRestartProcSlots.push_back(slotInfo);
        }
    }
}

bool SlotClassifier::hasFailedProcess(const SlotInfo *slotInfo) {
    for (size_t j = 0; j < slotInfo->processStatus.size(); j++) {
        if (slotInfo->processStatus[j].status
            == ProcessStatus::PS_FAILED)
        {
            return true;
        }
    }
    return false;
}

END_MASTER_FRAMEWORK_NAMESPACE(master_base);

