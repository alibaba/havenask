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
#include "master/HealthStatusTransfer.h"
#include "carbon/Log.h"
#include "master/HealthChecker.h"
#include "master/Lv7HealthChecker.h"

using namespace std;
BEGIN_CARBON_NAMESPACE(master);

HealthStatusTransfer::HealthStatusTransfer(int64_t lostTimeout) {
    _lostTimeout = lostTimeout;
}

HealthStatusTransfer::~HealthStatusTransfer() {
}

bool HealthStatusTransfer::transfer(
        bool touched, int64_t curTime, CheckResult *checkResult) const
{
    HealthType &status = checkResult->healthInfo.healthStatus;
    int64_t &lastLostTime = checkResult->lastLostTime;
    uint32_t &lostCount = checkResult->lostCount;
    HealthType curStatus = status;

    if (touched) {
        lostCount = 0;
        lastLostTime = 0;
    } else {
        lostCount++;
    }
    
    if (curStatus == HT_ALIVE) {
        if (touched) {
            return true;
        }
        if (lostCount < HEARTBEAT_LOST_COUNT_THRESHOLD) {
            return false;
        }
        status = HT_LOST;
        lastLostTime = curTime;
        return true;
    }
    
    if (curStatus == HT_UNKNOWN) {
        if (touched) {
            status = HT_ALIVE;
            return true;
        }
        if (lostCount < HEARTBEAT_LOST_COUNT_THRESHOLD) {
            return false;
        }
        status = HT_LOST;
        lastLostTime = curTime;
        return true;
    }

    if (curStatus == HT_LOST) {
        if (touched) {
            status = HT_ALIVE;
            return true;
        } 
        if (curTime - lastLostTime > _lostTimeout) {
            status = HT_DEAD;
        }
        return true;
    }

    if (curStatus == HT_DEAD) {
        if (touched) {
            status = HT_ALIVE;
        }
        return true;
    }

    return false;
}

END_CARBON_NAMESPACE(master);

