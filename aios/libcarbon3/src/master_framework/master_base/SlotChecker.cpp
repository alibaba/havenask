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
#include "master_base/SlotChecker.h"
#include "common/Log.h"
#include "autil/StringUtil.h"
#include "autil/EnvUtil.h"

using namespace std;
using namespace autil;
using namespace hippo;

BEGIN_MASTER_FRAMEWORK_NAMESPACE(master_base);

MASTER_FRAMEWORK_LOG_SETUP(master_base, SlotChecker);

#define DEFAULT_ABNORMAL_TIMEOUT (15 * 60 * 1000 * 1000L) // 15 min

SlotChecker::SlotChecker() {
    _abnormalTimeout = getAbnormalTimeout();
    MF_LOG(INFO, "set abnormal timeout:%ld", _abnormalTimeout);
}

SlotChecker::~SlotChecker() {
}

void SlotChecker::checkRunningSlots(const SlotInfos &slotInfos,
        set<SlotId> &longTimeNotRunningSlots)
{
    int64_t curTime = TimeUtility::currentTime();
    checkRunningSlots(slotInfos, curTime, longTimeNotRunningSlots);
}

void SlotChecker::checkRunningSlots(const SlotInfos &slotInfos,
                                    int64_t curTime,
                                    set<SlotId> &longTimeNotRunningSlots)
{
    MF_LOG(DEBUG, "check running slots, slot count:%zd.", slotInfos.size());
    map<SlotId, int64_t> tmpSlots;
    for (size_t i = 0; i < slotInfos.size(); i++) {
        const SlotInfo &slotInfo = slotInfos[i];
        const SlotId &slotId = slotInfo.slotId;
        bool allProcsRunning = isAllProcsRunning(slotInfo);
        MF_LOG(DEBUG, "slot[%s-%d-%ld] running:%d.",
               slotId.slaveAddress.c_str(), slotId.id,
               slotId.declareTime, (int32_t)allProcsRunning);
        if (allProcsRunning) {
            tmpSlots[slotId] = curTime;
            continue;
        }
        
        map<SlotId, int64_t>::iterator it = _lastRunningSlots.find(slotId);
        if (it == _lastRunningSlots.end()) {
            tmpSlots[slotId] = curTime;
        } else {
            int64_t lastRunningTime = it->second;
            tmpSlots[slotId] = lastRunningTime;
            if (curTime > lastRunningTime + _abnormalTimeout) {
                longTimeNotRunningSlots.insert(slotId);
                MF_LOG(INFO, "slot[%s-%d-%ld] long time not running. "
                       "curTime:%ld, lastRunningTime:%ld, timeout:%ld.",
                       slotId.slaveAddress.c_str(), slotId.id,
                       slotId.declareTime, curTime, lastRunningTime,
                       _abnormalTimeout);
            }
        }
    }
    _lastRunningSlots.swap(tmpSlots);
}

int64_t SlotChecker::getAbnormalTimeout() {
    int64_t val;
    if (!autil::EnvUtil::getEnvWithoutDefault(MF_ENV_ABNORMAL_TIMEOUT, val)) {
        MF_LOG(INFO, "not set abnormal timeout, use default.");
        return DEFAULT_ABNORMAL_TIMEOUT;
    }
    return val * 1000000;
}

bool SlotChecker::isAllProcsRunning(const SlotInfo &slotInfo) {
    if (slotInfo.processStatus.size() == 0) {
        return false;
    }
    
    for (size_t i = 0; i < slotInfo.processStatus.size(); i++) {
        if (slotInfo.processStatus[i].status != ProcessStatus::PS_RUNNING) {
            return false;
        }
    }
    return true;
}

END_MASTER_FRAMEWORK_NAMESPACE(master_base);

