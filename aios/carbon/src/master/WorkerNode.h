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
#ifndef CARBON_WORKERNODE_H
#define CARBON_WORKERNODE_H

/*
 * =============================================================
 * Several Principle of WorkerNode
 * 1. bind with hippo slot, auto release self if current slot
 *    can not reach the target plan
 * 2. the stats machine of slot status never go back
 * 3. reach target plan step by step
 * =============================================================
 */

#include "carbon/CommonDefine.h"
#include "carbon/RolePlan.h"
#include "carbon/Status.h"
#include "common/common.h"
#include "carbon/Log.h"
#include "master/HealthChecker.h"
#include "master/ServiceSwitch.h"
#include "master/ExtVersionedPlan.h"

BEGIN_CARBON_NAMESPACE(master);

class WorkerNode : public autil::legacy::Jsonizable
{
public:
    WorkerNode();
    WorkerNode(const nodeid_t &nodeGUID);
    virtual ~WorkerNode();
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json);

public:
    bool recover(const version_t &latestVersion,
                 const ExtVersionedPlanMap &versionedPlans);
    virtual void schedule();
    void setPlan(const version_t &version, const ExtVersionedPlan &plan);
    void setFinalPlan(const version_t &version, const ExtVersionedPlan &plan);
    void assignSlot(const SlotInfo &slotInfo);
    void updateSlotInfo(const SlotInfo &slotInfo);
    void updateHealthInfo(const HealthInfo &healthInfo);
    void updateServiceInfo(const ServiceInfo &serviceInfo);
    virtual void release();
    void releaseWithPref(ReleasePreference pref);
public:
    bool isAvailable() const;  //use to cal available matrix when rolling
    virtual bool isCompleted() const;  //the worker node is ready for target
    virtual bool isBroken() const;
    virtual bool inBadState() const;
    virtual const std::string getGeneralStateString() const;
    void fillWorkerNodeStatus(WorkerNodeStatus *workerNodeStatus) const;
    bool hasEmptySlotInfo() const;
    bool isNeedSlot() const;
public:
    bool isAssignedSlot() const;
    bool isUnAssignedSlot() const;
    bool canReleaseSlot() const;
    virtual bool isSlotReleased() const;
    bool isOffline() const { return _offline; }
    nodeid_t getId() const { return _nodeId; }
    const SlotInfo& getSlotInfo() const { return _slotInfo; }
    SlotId getSlotId() const { return _slotId; }
    std::string getIp() const;
    KVMap getHealthInfoMetas() const;
    KVMap getServiceInfoMetas() const;
    ReleasePreference getReleasePreference() const { return _pref; }    
    VersionedPlan getCurPlan() const { return _extCurPlan.plan; }
    VersionedPlan getFinalPlan() const { return _extFinalPlan.plan; }
    version_t getCurVersion() const { return _curVersion; }
    version_t getFinalVersion() const { return _finalVersion; }
    version_t getNextVersion() const { return _nextVersion; }
    tag_t getCurResourceTag() const { return _extCurPlan.resourceTag;}
    tag_t getFinalResourceTag() const { return _extCurPlan.resourceTag;}
    bool targetHasReached() const { return _targetHasReached; }
    std::string getProcessVersion() const { return _extCurPlan.processVersion; }

//todo: only use in compare func. to remove.
    HealthType getHealthStatus() const { return _healthInfo.healthStatus; }
    WorkerType getWorkerStatus() const { return _healthInfo.workerStatus; }
    ServiceType getServiceStatus() const { return _serviceInfo.status; }
    SlotType getSlotStatus() const { return _slotStatus; }
    int getServiceScore() const { return _serviceInfo.score; }
    bool isReclaiming() const;
    hippo::SlotPreference getSlotPreference() const { return _slotInfo.slotPreference; }

public: /* for test */
    SlotAllocStatus getSlotAllocStatus() const {
        return _slotAllocStatus;
    }
    void setSlotAllocStatus(SlotAllocStatus status) {
        _slotAllocStatus = status;
    }
    void setSlotId(const SlotId &slotId) {
        _slotId = slotId;
    }
    void setOfflineFlag(bool flag) {
        _offline = flag;
    }
private:
    void dealwithSlotUnAssigned();
    void dealwithSlotAssigned();
    void dealwithSlotLost();
    void dealwithSlotOfflining();
    void dealwithSlotReleasing();
    void dealwithSlotReleased();
    bool stepProcessUpdateGracefully();
    bool stepProcessResourcePlan();
    bool stepProcessLaunchPlan();
    bool stepProcessHealthInfo();
    bool stepProcessServiceInfo();
    bool isDead() const;
    bool isLongTimeNotMatchPlan() const;
    bool isUpdatingSlotResourceNotMatch() const;
    bool isLongTimeNotReady() const;
    bool resourcePlanMatch() const;
    bool launchPlanMatch() const;
    bool healthInfoMatch() const;
    bool serviceInfoMatch(bool online) const;
    bool healthInfoReady() const;
    void fixPref();
    void tryMoveToSlotReleasing();
public:
    enum AssignedScheduleStep {
        STEP_BEGIN = 0,
        STEP_PROCESS_UPDATE_GRACEFULLY,
        STEP_PROCESS_RESOURCE_PLAN,
        STEP_PROCESS_LAUNCH_PLAN,
        STEP_PROCESS_HEALTH_INFO,
        STEP_PROCESS_SERVICE_INFO,
    };
    AssignedScheduleStep _assignedStep;  //not use to make decision.
private:
    // plan, need persist
    nodeid_t _nodeId;
    version_t _curVersion;
    version_t _nextVersion;
    version_t _finalVersion;
    ExtVersionedPlan _extCurPlan;
    ExtVersionedPlan _extNextPlan;
    ExtVersionedPlan _extFinalPlan;
    SlotId _slotId;
    std::string _ip;
    bool _offline;
    bool _releasing;
    bool _targetHasReached;
    bool _updatingSlotResourceNotMatch;
    ReleasePreference _pref;
    int64_t _lastNotMatchTime;
    int64_t _lastNotReadyTime;
    SlotAllocStatus _slotAllocStatus;
    
    // runtime status
    SlotInfo _slotInfo;
    HealthInfo _healthInfo;
    ServiceInfo _serviceInfo;
    SlotType _slotStatus;
private:
    CARBON_LOG_DECLARE();
};

CARBON_TYPEDEF_PTR(WorkerNode);
typedef std::vector<WorkerNodePtr> WorkerNodeVect;

END_CARBON_NAMESPACE(master);

#endif //CARBON_WORKERNODE_H
