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
#include "master/SlotUtil.h"
#include "carbon/Log.h"
#include "hippo/HippoUtil.h"
#include <sstream>
#include "autil/StringUtil.h"
#include "master/SignatureGenerator.h"
#include "carbon/CommonDefine.h"
#include <rapidjson/document.h>
#include <rapidjson/writer.h>
#include <rapidjson/stringbuffer.h>
#include "autil/HashAlgorithm.h"
#include "autil/StringUtil.h"

using namespace std;
using namespace autil;
using namespace rapidjson;
BEGIN_CARBON_NAMESPACE(master);
USE_CARBON_NAMESPACE(common);

CARBON_LOG_SETUP(master, SlotUtil);

SlotUtil::SlotUtil() {
}

SlotUtil::~SlotUtil() {
}

bool SlotUtil::isEmptySlot(const SlotId &slotId) {
    return (slotId.slaveAddress == "" &&
            slotId.id == -1 &&
            slotId.declareTime == 0);
}

bool SlotUtil::isPackageReady(const std::vector<PackageInfo> &packageInfos,
                               const SlotInfo &slotInfo)
{
    if (slotInfo.packageStatus.status !=
        PackageStatus::IS_INSTALLED)
    {
        return false;
    }

    string localPackageChecksum =
        hippo::HippoUtil::genPackageChecksum(packageInfos);

    CARBON_LOG(DEBUG, "local package checksum:[%s], "
               "slot package checksum:[%s]",
               localPackageChecksum.c_str(),
               slotInfo.packageChecksum.c_str());
    return localPackageChecksum == slotInfo.packageChecksum;
}
    
bool SlotUtil::isProcessReady(const std::vector<ProcessInfo> &processInfos,
                               const SlotInfo &slotInfo)
{
    map<string, ProcessStatus> procStatusMap;
    const vector<ProcessStatus> &procStatusVect = slotInfo.processStatus;
    for (size_t i = 0; i < procStatusVect.size(); i++) {
        procStatusMap[procStatusVect[i].processName] = procStatusVect[i];
    }

    if (procStatusMap.size() != processInfos.size()) {
        CARBON_LOG(WARN, "processInfos.size() != processStatus.size()");
        return false;
    }

    for (const auto &procInfo : processInfos) {
        const string &procName = procInfo.name;
        map<string, ProcessStatus>::iterator procStatusIt =
            procStatusMap.find(procName);
        if (procStatusIt == procStatusMap.end()) {
            CARBON_LOG(WARN, "proc status not find, proc:[%s].", procName.c_str());
            return false;
        }

        const ProcessStatus &procStatus = procStatusIt->second;
        if (procInfo.instanceId != procStatus.instanceId) {
            CARBON_LOG(WARN, "proc instanceid not match, instacnceid(cur/expect):(%ld/%ld)",
                       procStatus.instanceId, procInfo.instanceId);
            return false;
        }

        if (procInfo.isDaemon) {
            if (procStatus.status != ProcessStatus::PS_RUNNING) {
                CARBON_LOG(WARN, "proc status not match, status(cur/expect):%d vs %d.",
                        (int)procStatus.status, (int)ProcessStatus::PS_RUNNING);
                return false;
            }
        }
    }
    
    return true;
}

bool SlotUtil::isDataReady(const std::vector<DataInfo> &dataInfos,
                           const SlotInfo &slotInfo)
{
    map<string, DataStatus> dataStatusMap;
    const vector<DataStatus> dataStatusVect = slotInfo.dataStatus;
    for (size_t i = 0; i < dataStatusVect.size(); i++) {
        dataStatusMap[dataStatusVect[i].name] = dataStatusVect[i];
    }

    if (dataInfos.size() != dataStatusMap.size()) {
        CARBON_LOG(INFO, "data items size not match, expect:%zd, actual:%zd",
                   dataInfos.size(), dataStatusMap.size());
        return false;
    }
    
    for (const auto &dataInfo : dataInfos) {
        const string &name = dataInfo.name;
        map<string, DataStatus>::iterator dataStatusIt =
            dataStatusMap.find(name);
        if (dataStatusIt == dataStatusMap.end()) {
            CARBON_LOG(INFO, "no corresponding data info for %s", name.c_str());
            return false;
        }

        const DataStatus dataStatus = dataStatusIt->second;
        CARBON_LOG(DEBUG, "data [%s] status:%d, attemptId:%d, curVersion:%d",
                   name.c_str(), (int)dataStatus.status, (int)dataStatus.attemptId,
                   (int)dataStatus.curVersion);
        if (dataStatus.status != DataStatus::FINISHED ||
            dataStatus.attemptId != dataInfo.attemptId)
        {
            return false;
        } else {
            if (dataInfo.version == -1) {
                return dataStatus.curVersion == 0;
            } else {
                return dataStatus.curVersion == dataInfo.version;
            }
        }
    }
    return true;
}

bool SlotUtil::requirementIdMatch(const ExtVersionedPlan &plan,
                              const SlotInfo &slotInfo)
{
    return plan.resourceChecksum == slotInfo.requirementId;
}

bool SlotUtil::isSlotInfoMatchingResourcePlan(const SlotInfo &slotInfo)
{
    return (!slotInfo.noLongerMatchQueue) &&
        (!slotInfo.noLongerMatchResourceRequirement);
}
 
//TODO support pod matching plan
bool SlotUtil::isSlotInfoMatchingPlan(const LaunchPlan &launchPlan,
                                      const SlotInfo &slotInfo)
{
    if (launchPlan.podDesc != "") {
        return isPodDescMatch(launchPlan, slotInfo);
    } else {
        return isProcessContextMatch(launchPlan, slotInfo);
    }
}

bool SlotUtil::extractContainerDescs(
        const std::string &podDesc, vector<Document*> *containerDescs, Document** mainContainerDesc)
{
    Document *container = new Document;
    if (!RapidJsonUtil::fromJson(podDesc, *container)) {
        delete container;
        return false;
    }
    containerDescs->push_back(container);
    (*mainContainerDesc) = container;
    
    if (container->HasMember("PodExtendedContainers")) {
        const Value &extendedContaianers = (*container)["PodExtendedContainers"];
        if (!extendedContaianers.IsArray()) {
            return false;
        }
        for (size_t i = 0; i < extendedContaianers.Size(); i++) {
            Document *containerDoc = new Document;
            if (!RapidJsonUtil::convertToDoc(extendedContaianers[i], *containerDoc)) {
                delete containerDoc;
                for (size_t j = 0; j < containerDescs->size(); j++) {
                    delete (*containerDescs)[j];
                }
                (*containerDescs).clear();
                return false;
            }
            containerDescs->push_back(containerDoc);
        }
    }
    return true;
}

bool SlotUtil::isPodDescMatch(const LaunchPlan &launchPlan,
                              const SlotInfo &slotInfo)
{
    vector<Document*> containerDescs;
    Document* mainContainerDesc = nullptr;
    if (!extractContainerDescs(launchPlan.podDesc, &containerDescs, &mainContainerDesc)) {
        return false;
    }

    set<string> containerInstanceNames;
    bool isMatch = true;
    for (size_t i = 0; i < containerDescs.size(); i++) {
        if (!containerDescs[i]->HasMember("InstanceId")) {
            isMatch = false;
            break;
        }
        string instId = (*containerDescs[i])["InstanceId"].GetString();
        containerInstanceNames.insert(instId);
    }

    // only check the main container's IsDaemon
    bool isDaemon = true;
    if (mainContainerDesc && mainContainerDesc->HasMember("IsDaemon")) {
        isDaemon = (*mainContainerDesc)["IsDaemon"].GetBool();
    }

    map<string, ProcessStatus> procStatusMap;
    const vector<ProcessStatus> &procStatusVect = slotInfo.processStatus;
    for (size_t i = 0; i < procStatusVect.size(); i++) {
        procStatusMap[procStatusVect[i].processName] = procStatusVect[i];
    }

    if (procStatusMap.size() != containerInstanceNames.size()) {
        CARBON_LOG(WARN,
                   "containerInstanceNames.size() != containerInstanceStatus.size()");
        isMatch = false;
    }

    for (const auto &containerInstanceName : containerInstanceNames) {
        if (!isMatch) {
            break;
        }
        map<string, ProcessStatus>::iterator procStatusIt =
            procStatusMap.find(containerInstanceName);
        if (procStatusIt == procStatusMap.end()) {
            CARBON_LOG(WARN, "container status not find, containerInstanceId:[%s].",
                       containerInstanceName.c_str());
            isMatch = false;
        }

        if (isDaemon) {
            const ProcessStatus &procStatus = procStatusIt->second;
            if (procStatus.status != ProcessStatus::PS_RUNNING) {
                CARBON_LOG(WARN, "container instance not match");
                isMatch = false;
            }
        } else {
            const ProcessStatus &procStatus = procStatusIt->second;
            if (procStatus.status == ProcessStatus::PS_UNKNOWN) {
                isMatch = false;
            }
        }
    }
    
    for (size_t i = 0; i < containerDescs.size(); i++) {
        delete containerDescs[i];
    }
    containerDescs.clear();
    return isMatch;
}

bool SlotUtil::isProcessContextMatch(const LaunchPlan &launchPlan,
                                     const SlotInfo &slotInfo)
{
    return (SlotUtil::isPackageReady(launchPlan.packageInfos, slotInfo) &&
            SlotUtil::isProcessReady(launchPlan.processInfos, slotInfo) &&
            SlotUtil::isDataReady(launchPlan.dataInfos, slotInfo));
}

string SlotUtil::toString(const SlotId &slotId) {
    stringstream ss;
    ss << slotId.slaveAddress << "-"
       << slotId.id << "-"
       << slotId.declareTime;
    return ss.str();
}
    
string SlotUtil::toString(const SlotInfo &slotInfo) {
    assert(false);
    return "";
}

bool SlotUtil::hasFailedDataStatus(const SlotInfo &slotInfo) {
    for (size_t i = 0; i < slotInfo.dataStatus.size(); i++) {
        if (slotInfo.dataStatus[i].status == DataStatus::FAILED) {
            return true;
        }
    }
    return false;
}

bool SlotUtil::isSlotUnRecoverable(const SlotInfo &slotInfo) {
    return slotInfo.packageStatus.status == PackageStatus::IS_FAILED ||
        hasFailedProcess(slotInfo) ||
        hasFailedDataStatus(slotInfo);
}

bool SlotUtil::isPackageInstalled(const SlotInfo &slotInfo) {
    return slotInfo.packageStatus.status == PackageStatus::IS_INSTALLED;
}

bool SlotUtil::isAllProcessAlive(const SlotInfo &slotInfo) {
    set<ProcessStatus::Status> allowedStatus = {
        ProcessStatus::PS_RUNNING,
        ProcessStatus::PS_STOPPING
    };
    return checkAllProcessStatus(slotInfo, allowedStatus);
}

bool SlotUtil::isAllProcessRunning(const SlotInfo &slotInfo) {
    set<ProcessStatus::Status> allowedStatus = {ProcessStatus::PS_RUNNING};
    return checkAllProcessStatus(slotInfo, allowedStatus);
}

bool SlotUtil::checkAllProcessStatus(const SlotInfo &slotInfo,
                                     const set<ProcessStatus::Status> &allowedStatus)
{
    if (slotInfo.processStatus.size() == 0) {
        return false;
    }
    
    for (size_t j = 0; j < slotInfo.processStatus.size(); j++) {
        if (allowedStatus.find(slotInfo.processStatus[j].status) == allowedStatus.end()) {
            return false;
        }
    }
    return true;    
}

std::string SlotUtil::getIp(const SlotInfo &slotInfo) {
    return hippo::HippoUtil::getIp(slotInfo);
}

SlotStatus SlotUtil::extractSlotStatus(const SlotInfo &slotInfo) {
    SlotType status = ST_UNKNOWN;
    
    if (slotInfo.slaveStatus.status == SlaveStatus::DEAD) {
        status = ST_DEAD;
    } else if (slotInfo.packageStatus.status == PackageStatus::IS_FAILED) {
        status = ST_PKG_FAILED;
    } else if (SlotUtil::hasFailedProcess(slotInfo)) {
        status = ST_PROC_FAILED;
    } else if (SlotUtil::isAllProcessRunning(slotInfo)) {
        status = ST_PROC_RUNNING;
    } else if (SlotUtil::hasTerminatedProcess(slotInfo)) {
        // check the TERMINATED process before the RESTARTING,
        // when the slot has many processes
        status = ST_PROC_TERMINATED;
    } else if (SlotUtil::hasRestartingProcess(slotInfo)) {
        status = ST_PROC_RESTARTING;
    } else {
        status = ST_UNKNOWN;
    }

    SlotStatus slotStatus;
    slotStatus.status = status;
    return slotStatus;
}

bool SlotUtil::hasProcessWithStatus(const SlotInfo &slotInfo,
                                    ProcessStatus::Status procStatus)
{
    for (size_t i = 0; i < slotInfo.processStatus.size(); i++) {
        if (slotInfo.processStatus[i].status == procStatus) {
            return true;
        }
    }
    return false;
}

bool SlotUtil::hasFailedProcess(const SlotInfo &slotInfo) {
    return hasProcessWithStatus(slotInfo, ProcessStatus::PS_FAILED);
}

bool SlotUtil::hasTerminatedProcess(const SlotInfo &slotInfo) {
    return hasProcessWithStatus(slotInfo, ProcessStatus::PS_TERMINATED);
}

bool SlotUtil::hasRestartingProcess(const SlotInfo &slotInfo) {
    return hasProcessWithStatus(slotInfo, ProcessStatus::PS_RESTARTING);
}

void SlotUtil::rewriteProcInstanceId(LaunchPlan *launchPlan,
                                     const string &extraSuffix)
{
    for (size_t i = 0; i < launchPlan->processInfos.size(); i++) {
        ProcessInfo &procInfo = launchPlan->processInfos[i];
        if (procInfo.instanceId == 0) {
            int64_t procInstId = SignatureGenerator::genProcInstanceId(
                    procInfo, extraSuffix);
            procInfo.instanceId = procInstId;
        }
    }
}

void SlotUtil::rewritePackageCheckSum(LaunchPlan *launchPlan) {
    string packageCheckSum = StringUtil::toString(
            SignatureGenerator::genPackageCheckSum(launchPlan->packageInfos));
    for (size_t i = 0; i < launchPlan->processInfos.size(); i++) {
        ProcessInfo &procInfo = launchPlan->processInfos[i];
        procInfo.envs.push_back({ENV_PACKAGE_CHECKSUM, packageCheckSum});
    }
}

string SlotUtil::extractProcessVersion(const LaunchPlan &launchPlan) {
    // packageCheckSum:instanceId1#instanceId2....
    string processVersion = "";
    string packageCheckSum = StringUtil::toString(
            SignatureGenerator::genPackageCheckSum(launchPlan.packageInfos));
    processVersion = packageCheckSum + ":";
    for (size_t i = 0; i < launchPlan.processInfos.size(); i++) {
        if (i != 0) {
            processVersion += "#";
        }
        const ProcessInfo &procInfo = launchPlan.processInfos[i];
        processVersion += StringUtil::toString(procInfo.instanceId);
    }
    return processVersion;
}

END_CARBON_NAMESPACE(master);
