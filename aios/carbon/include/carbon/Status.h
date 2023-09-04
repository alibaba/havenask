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
#ifndef CARBON_STATUS_H
#define CARBON_STATUS_H

#include "CommonDefine.h"
#include "RolePlan.h"
#include "autil/legacy/jsonizable.h"

namespace carbon {

enum SlotType {
    ST_PKG_FAILED = 0,
    ST_UNKNOWN,
    ST_DEAD,
    ST_PROC_FAILED,
    ST_PROC_RESTARTING,
    ST_PROC_RUNNING,
    ST_PROC_TERMINATED
};

#define CHECK_TYPE_EXIST(type, typeArray)                               \
    do {                                                                \
        if ((int)type >= sizeof(typeArray) / sizeof(typeArray[0])) {    \
            return "UNDEFINED";                                         \
        }                                                               \
    } while(0)

static inline const char* enumToCStr(SlotType type) {
    static const char* typeArray[] = {"ST_PKG_FAILED",
                                      "ST_UNKNOWN",
                                      "ST_DEAD",
                                      "ST_PROC_FAILED",
                                      "ST_PROC_RESTARTING",
                                      "ST_PROC_RUNNING",
                                      "ST_PROC_TERMINATED"};
    CHECK_TYPE_EXIST(type, typeArray);
    return typeArray[type];
}

enum WorkerModeType {
    MT_ACTIVE = 0,
    MT_HOT_STANDBY,
    MT_WARM_STANDBY,
    MT_COLD_STANDBY
};

static inline const char* enumToCStr(WorkerModeType type) {
    static const char* typeArray[] = {"active",
                                      "hotStandBy",
                                      "warmStandBy",
                                      "coldStandBy"};
    CHECK_TYPE_EXIST(type, typeArray);
    return typeArray[type];
}

enum HealthType {
    HT_UNKNOWN = 0,
    HT_LOST,
    HT_ALIVE,
    HT_DEAD
};

static inline const char* enumToCStr(HealthType type) {
    static const char* typeArray[] = {"HT_UNKNOWN",
                                      "HT_LOST",
                                      "HT_ALIVE",
                                      "HT_DEAD"};
    CHECK_TYPE_EXIST(type, typeArray);
    return typeArray[type];
}

enum WorkerType {
    WT_UNKNOWN = 0,
    WT_NOT_READY,
    WT_READY
};

static inline const char* enumToCStr(WorkerType type) {
    static const char* typeArray[] = {"WT_UNKNOWN",
                                      "WT_NOT_READY",
                                      "WT_READY"};
    CHECK_TYPE_EXIST(type, typeArray);
    return typeArray[type];
}

enum ServiceType {
    SVT_UNKNOWN = 0,
    SVT_UNAVAILABLE,
    SVT_PART_AVAILABLE,
    SVT_AVAILABLE,
};

static inline const char* enumToCStr(ServiceType type) {
    static const char* typeArray[] = {"SVT_UNKNOWN",
                                      "SVT_UNAVAILABLE",
                                      "SVT_PART_AVAILABLE",
                                      "SVT_AVAILABLE"};
    CHECK_TYPE_EXIST(type, typeArray);
    return typeArray[type];
}

JSONIZABLE_CLASS(HealthInfo) {
public:
    HealthType healthStatus;
    WorkerType workerStatus;
    KVMap metas;
    version_t version;

    HealthInfo() {
        healthStatus = HT_UNKNOWN;
        workerStatus = WT_UNKNOWN;
    }

    JSONIZE() {
        JSON_FIELD(healthStatus);
        JSON_FIELD(workerStatus);
        JSON_FIELD(metas);
        JSON_FIELD(version);
    }
};

JSONIZABLE_CLASS(ServiceInfo) {
public:
    ServiceInfo() {
        status = SVT_UNKNOWN;
        score = 0;
    }

    ServiceType status;
    KVMap metas;
    int score;

    JSONIZE() {
        JSON_FIELD(status);
        JSON_FIELD(metas);
        JSON_FIELD(score);
    }
};

enum SlotAllocStatus {
    SAS_UNASSIGNED = 0,
    SAS_ASSIGNED,
    SAS_LOST,
    SAS_OFFLINING,
    SAS_RELEASING,
    SAS_RELEASED
};

static inline const char* enumToCStr(SlotAllocStatus type) {
    static const char* typeArray[] = {"SAS_UNASSIGNED",
                                      "SAS_ASSIGNED",
                                      "SAS_LOST",
                                      "SAS_OFFLINING",
                                      "SAS_RELEASING",
                                      "SAS_RELEASED"};
    CHECK_TYPE_EXIST(type, typeArray);
    return typeArray[type];
}

JSONIZABLE_CLASS(SlotStatus) {
public:
    SlotType status;
    SlotId slotId;

    JSONIZE() {
        JSON_FIELD(status);
        JSON_FIELD(slotId);
    }

    SlotStatus() {
        status = ST_UNKNOWN;
    }
};

JSONIZABLE_CLASS(ProcessStatusJsonizeWrapper) {
public:
    ProcessStatusJsonizeWrapper(const ProcessStatus &processStatus) {
        _processStatus = processStatus;
    }
    ProcessStatusJsonizeWrapper() {}
    ProcessStatus _processStatus;
    JSONIZE() {
        JSON_NAME_FIELD("isDaemon", _processStatus.isDaemon);
        JSON_NAME_FIELD("status", _processStatus.status);
        JSON_NAME_FIELD("processName", _processStatus.processName);
        JSON_NAME_FIELD("restartCount", _processStatus.restartCount);
        JSON_NAME_FIELD("startTime", _processStatus.startTime);
        JSON_NAME_FIELD("exitCode", _processStatus.exitCode);
        JSON_NAME_FIELD("pid", _processStatus.pid);
        JSON_NAME_FIELD("instanceId", _processStatus.instanceId);
    }
};

JSONIZABLE_CLASS(SlotInfoJsonizeWrapper) {
public:
    SlotInfoJsonizeWrapper(const SlotInfo &slotInfo){
        _slotInfo = slotInfo;
    }
    SlotInfoJsonizeWrapper(){}
    SlotInfo _slotInfo;

    JSONIZE() {
        JSON_NAME_FIELD("role", _slotInfo.role);
        JSON_NAME_FIELD("slotId", _slotInfo.slotId);
        JSON_NAME_FIELD("reclaiming", _slotInfo.reclaiming);
        JSON_NAME_FIELD("slotResource", _slotInfo.slotResource);
        JSON_NAME_FIELD("slaveStatus", _slotInfo.slaveStatus.status);
        JSON_NAME_FIELD("packageStatus", _slotInfo.packageStatus.status);
        JSON_NAME_FIELD("packageChecksum", _slotInfo.packageChecksum);
        JSON_NAME_FIELD("launchSignature", _slotInfo.launchSignature);
        JSON_NAME_FIELD("noLongerMatchResourceRequirement",
                     _slotInfo.noLongerMatchResourceRequirement);
        std::vector<ProcessStatusJsonizeWrapper> procs;
        if (json.GetMode() == autil::legacy::Jsonizable::FROM_JSON) {
            JSON_NAME_FIELD("processStatus", procs);
            for (size_t i = 0; i < procs.size(); ++i) {
                _slotInfo.processStatus.push_back(procs[i]._processStatus);
            }
        } else {
            for (size_t i = 0; i < _slotInfo.processStatus.size(); i++) {
                procs.push_back(ProcessStatusJsonizeWrapper(_slotInfo.processStatus[i]));
            }
            json.Jsonize("processStatus", procs);
        }
    }
};

JSONIZABLE_CLASS(WorkerNodeStatus) {
public:
    nodeid_t replicaNodeId;
    nodeid_t workerNodeId;
    version_t curVersion;
    version_t nextVersion;
    version_t finalVersion;
    bool offline;
    bool releasing;
    bool reclaiming;
    bool readyForCurVersion;
    bool isBackup;
    int64_t lastNotMatchTime;
    int64_t lastNotReadyTime;
    SlotAllocStatus slotAllocStatus;
    SlotInfo slotInfo;
    HealthInfo healthInfo;
    ServiceInfo serviceInfo;
    SlotStatus slotStatus;

    std::string ip;
    std::string userDefVersion;
    std::string targetSignature;
    std::string targetCustomInfo;
    std::string workerMode;
    std::map<std::string,std::string> labels;

    JSONIZE() {
        JSON_FIELD(workerNodeId);
        JSON_FIELD(curVersion);
        JSON_FIELD(nextVersion);
        JSON_FIELD(finalVersion);
        JSON_FIELD(offline);
        JSON_FIELD(releasing);
        JSON_FIELD(reclaiming);
        JSON_FIELD(readyForCurVersion);
        JSON_FIELD(lastNotMatchTime);
        JSON_FIELD(lastNotReadyTime);
        JSON_FIELD(slotAllocStatus);
        JSON_FIELD(slotStatus);
        JSON_FIELD(healthInfo);
        JSON_FIELD(serviceInfo);
        JSON_FIELD(ip);
        JSON_FIELD(userDefVersion);
        JSON_FIELD(targetSignature);
        JSON_FIELD(targetCustomInfo);
        JSON_FIELD(workerMode);
        JSON_FIELD(labels);

        if (json.GetMode() == autil::legacy::Jsonizable::TO_JSON) {
            SlotInfoJsonizeWrapper slotInfoWrapper(slotInfo);
            json.Jsonize("slotInfo", slotInfoWrapper);
        } else {
            SlotInfoJsonizeWrapper slotInfoWrapper;
            JSON_NAME_FIELD("slotInfo", slotInfoWrapper);
            slotInfo = slotInfoWrapper._slotInfo;
        }
    }

    WorkerNodeStatus() {
        lastNotMatchTime = 0;
        lastNotReadyTime = 0;
        offline = false;
        releasing = false;
        reclaiming = false;
        readyForCurVersion = false;
        isBackup = false;
        workerMode = enumToCStr(WorkerModeType(MT_ACTIVE));
        slotAllocStatus = SAS_UNASSIGNED;
    }

    bool valid() const {
        return slotAllocStatus != SAS_UNASSIGNED
            && slotAllocStatus != SAS_RELEASED; // cur released but exist
    }
};

JSONIZABLE_CLASS(ReplicaNodeStatus) {
public:
    nodeid_t replicaNodeId;
    WorkerNodeStatus curWorkerNodeStatus;
    WorkerNodeStatus backupWorkerNodeStatus;
    int64_t timeStamp;
    std::string userDefVersion;
    bool readyForCurVersion;

    JSONIZE() {
        JSON_FIELD(replicaNodeId);
        JSON_FIELD(curWorkerNodeStatus);
        JSON_FIELD(backupWorkerNodeStatus);
        JSON_FIELD(timeStamp);
        JSON_FIELD(userDefVersion);
        JSON_FIELD(readyForCurVersion);
    }

    ReplicaNodeStatus() {
        timeStamp = 0;
        readyForCurVersion = false;
    }

    std::vector<WorkerNodeStatus> getValidWorkerNodeStatus() const {
        std::vector<WorkerNodeStatus> workerNodeStatusVec;
        if (curWorkerNodeStatus.valid()) {
            workerNodeStatusVec.push_back(curWorkerNodeStatus);
            workerNodeStatusVec.back().isBackup = false;
            workerNodeStatusVec.back().replicaNodeId = replicaNodeId;
        }
        if (backupWorkerNodeStatus.valid()) {
            workerNodeStatusVec.push_back(backupWorkerNodeStatus);
            workerNodeStatusVec.back().isBackup = true;
            workerNodeStatusVec.back().replicaNodeId = replicaNodeId;
        }
        return workerNodeStatusVec;
    }
};

JSONIZABLE_CLASS(RoleStatus) {
public:
    roleid_t roleId;
    GlobalPlan globalPlan;
    std::map<version_t, VersionedPlan> versionedPlans;
    version_t latestVersion;
    std::vector<ReplicaNodeStatus> nodes;
    std::string userDefVersion;
    bool readyForCurVersion;
    int32_t adjustedCount;
    int32_t adjustedMinHealthCapacity;

    JSONIZE() {
        JSON_FIELD(roleId);
        JSON_FIELD(globalPlan);
        JSON_FIELD(versionedPlans);
        JSON_FIELD(latestVersion);
        JSON_FIELD(nodes);
        JSON_FIELD(userDefVersion);
        JSON_FIELD(readyForCurVersion);
        JSON_FIELD(adjustedCount);
        JSON_FIELD(adjustedMinHealthCapacity);
    }

    RoleStatus() {
        readyForCurVersion = false;
        adjustedMinHealthCapacity = 0;
        adjustedCount = 0;
    }
};

JSONIZABLE_CLASS(GroupStatus) {
public:
    groupid_t groupId;
    std::map<roleid_t, RoleStatus> roles;

    JSONIZE() {
        JSON_FIELD(groupId);
        JSON_FIELD(roles);
    }
};

JSONIZABLE_CLASS(CarbonInfo) {
public:
    std::string appId;
    std::string carbonZk;
    std::string hippoZk;
    std::string releaseVersion;
    std::string buildVersion;
    std::string mode; // lib or binary
    bool isInited;

    CarbonInfo() {
        isInited = false;
    }

    JSONIZE() {
        JSON_FIELD(appId);
        JSON_FIELD(carbonZk);
        JSON_FIELD(hippoZk);
        JSON_FIELD(releaseVersion);
        JSON_FIELD(buildVersion);
        JSON_FIELD(mode);
        JSON_FIELD(isInited);
    }
};

JSONIZABLE_CLASS(BufferSlotStatus) {
public:
    int32_t bufferSlotCount;
    int32_t inUseSlotCount;
    std::vector<SlotStatus> bufferSlots;
    std::map<int64_t, size_t> slotLaunchPlanSigCounts; // <launch_plan_sig, slot_cnt>

    BufferSlotStatus() {
        bufferSlotCount = 0;
        inUseSlotCount = 0;
    }

    JSONIZE() {
        JSON_FIELD(bufferSlotCount);
        JSON_FIELD(inUseSlotCount);
        JSON_FIELD(bufferSlots);
        JSON_FIELD(slotLaunchPlanSigCounts);
    }
};

JSONIZABLE_CLASS(DriverStatus) {
public:
    BufferSlotStatus bufferStatus;
    bool useBuffer;
    
    DriverStatus() : useBuffer(false) {}

    JSONIZE() {
        JSON_FIELD(useBuffer);
        JSON_FIELD(bufferStatus);
    }
};

}

#endif
