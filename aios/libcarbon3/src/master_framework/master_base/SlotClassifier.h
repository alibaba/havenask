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
#ifndef MASTER_FRAMEWORK_SLOTCLASSIFIER_H
#define MASTER_FRAMEWORK_SLOTCLASSIFIER_H

#include "master_framework/common.h"
#include "hippo/DriverCommon.h"

BEGIN_MASTER_FRAMEWORK_NAMESPACE(master_base);

typedef std::vector<hippo::SlotInfo> SlotInfos;

class SlotClassifier
{
public:
    SlotClassifier();
    ~SlotClassifier();
private:
    SlotClassifier(const SlotClassifier &);
    SlotClassifier& operator=(const SlotClassifier &);

private:
    static bool hasFailedProcess(const hippo::SlotInfo *slotInfo);
    
public:
    static void classifySlots(
            const SlotInfos &slots,
            std::vector<const hippo::SlotInfo*> &onlineSlots,
            std::vector<const hippo::SlotInfo*> &needReleaseSlots,
            std::vector<const hippo::SlotInfo*> &needResetSlots,
            std::vector<const hippo::SlotInfo*> &needRestartProcSlots,
            bool reclaimingAutoRelease,
            bool longTimeNotRunningAutoRelease,
            const std::set<hippo::SlotId> &longTimeNotRunningSlots = std::set<hippo::SlotId>());
};

MASTER_FRAMEWORK_TYPEDEF_PTR(SlotClassifier);

END_MASTER_FRAMEWORK_NAMESPACE(master_base);

#endif //MASTER_FRAMEWORK_SLOTCLASSIFIER_H
