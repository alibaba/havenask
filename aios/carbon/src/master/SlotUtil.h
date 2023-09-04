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
#ifndef CARBON_SLOTUTIL_H
#define CARBON_SLOTUTIL_H

#include "common/common.h"
#include "carbon/Log.h"
#include "carbon/RolePlan.h"
#include "carbon/Status.h"
#include "master/ExtVersionedPlan.h"
#include "common/RapidJsonUtil.h"
#include <rapidjson/document.h>
#include <rapidjson/writer.h>
#include <rapidjson/stringbuffer.h>

BEGIN_CARBON_NAMESPACE(master);

class SlotUtil
{
public:
    SlotUtil();
    ~SlotUtil();
private:
    SlotUtil(const SlotUtil &);
    SlotUtil& operator=(const SlotUtil &);
public:
    static bool isEmptySlot(const SlotId &slotId);

    static bool isPackageReady(const std::vector<PackageInfo> &packageInfos,
                               const SlotInfo &slotInfo);

    static bool isProcessReady(const std::vector<ProcessInfo> &processInfos,
                               const SlotInfo &slotInfo);
    
    static bool isDataReady(const std::vector<DataInfo> &dataInfos,
                            const SlotInfo &slotInfo);

    static bool isSlotInfoMatchingPlan(const LaunchPlan &launchPlan,
            const SlotInfo &slotInfo);

    static bool isSlotInfoMatchingResourcePlan(const SlotInfo &slotInfo);

    static bool requirementIdMatch(const ExtVersionedPlan &plan,
                                   const SlotInfo &slotInfo);

    static std::string toString(const SlotId &slotId);
    
    static std::string toString(const SlotInfo &slotInfo);
    
    static bool hasProcessWithStatus(const SlotInfo &slotInfo,
            ProcessStatus::Status procStatus);

    static bool hasFailedProcess(const SlotInfo &slotInfo);

    static bool hasTerminatedProcess(const SlotInfo &slotInfo);

    static bool hasFailedDataStatus(const SlotInfo &slotInfo);

    static bool hasRestartingProcess(const SlotInfo &slotInfo);

    static bool isSlotUnRecoverable(const SlotInfo &slotInfo);

    static bool isPackageInstalled(const SlotInfo &slotInfo);
    static bool checkAllProcessStatus(const SlotInfo &slotInfo,
            const std::set<ProcessStatus::Status> &allowedStatus);
    static bool isAllProcessAlive(const SlotInfo &slotInfo);
    static bool isAllProcessRunning(const SlotInfo &slotInfo);

    static std::string getIp(const SlotInfo &slotInfo);

    static SlotStatus extractSlotStatus(const SlotInfo &slotInfo);

    static void rewriteProcInstanceId(LaunchPlan *launchPlan,
            const std::string &extraSuffix = "");

    static bool isPodDescMatch(const LaunchPlan &launchPlan,
                               const SlotInfo &slotInfo);

    static bool isProcessContextMatch(const LaunchPlan &launchPlan,
            const SlotInfo &slotInfo);

    static bool extractContainerDescs(
            const std::string &podDesc,
            std::vector<rapidjson::Document*> *containerDescs,
            rapidjson::Document** mainContainerDesc);
    
    static void rewritePackageCheckSum(LaunchPlan *launchPlan);

    static std::string extractProcessVersion(const LaunchPlan &launchPlan);
private:
    CARBON_LOG_DECLARE();
};

CARBON_TYPEDEF_PTR(SlotUtil);

END_CARBON_NAMESPACE(master);

#endif //CARBON_SLOTUTIL_H
