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
#ifndef CARBON_ROLE_H
#define CARBON_ROLE_H

#include "common/common.h"
#include "carbon/Log.h"
#include "carbon/RolePlan.h"
#include "carbon/Ops.h"
#include "master/ReplicaNode.h"
#include "master/HippoAdapter.h"
#include "master/NodeSelector.h"
#include "master/HealthChecker.h"
#include "master/ServiceSwitch.h"
#include "master/HealthCheckerManager.h"
#include "master/ServiceSwitchManager.h"
#include "master/RoleExecutor.h"
#include "master/ReplicaNodeCreator.h"
#include "master/ExtVersionedPlan.h"
#include "autil/legacy/jsonizable.h"


BEGIN_CARBON_NAMESPACE(master);

typedef std::map<version_t, int32_t> PercentMap;

struct ScheduleParams {
    ScheduleParams() {
        maxCount = -1;
        minHealthCount = -1;
        timeStamp = 0;
    }
    
    std::map<version_t, int32_t> holdingCountMap;
    int32_t minHealthCount;
    int32_t maxCount;
    int64_t timeStamp;
};

class Role : public autil::legacy::Jsonizable
{
public:
    Role();
    
    Role(const groupid_t &groupId, const roleid_t &roleId, const roleguid_t &roleGUID);

    virtual ~Role();

public:
    Role(const Role &rhs);

    Role& operator = (const Role& rhs);
    
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json);
    
public:
    bool init(const HippoAdapterPtr &hippoAdapterPtr,
              const HealthCheckerManagerPtr &healthCheckerManagerPtr,
              const ServiceSwitchManagerPtr &serviceSwitchManagerPtr,
              bool recover = false);

    /* virtual for mock */
    virtual void setPlan(const version_t &version, const RolePlan &rolePlan);

    void setEmptyPlan(const version_t &version);
    
    /* virtual for mock */
    virtual bool isStopped() const;

    void stop();

    /* virtual for mock */
    virtual void schedule(const ScheduleParams &scheduleParams);

    /* virtual for mock */
    virtual void update();

    /* virtual for mock */
    virtual void execute();
    
    const ReplicaNodeVect& getReplicaNodes() const;

    std::map<version_t, int32_t> getOldVerCountMap() const;
    
    /* virtual for mock */
    virtual void getVerAvailablePercent(PercentMap *verAvailablePercent) const;

    int32_t getTargetCount() const { return _globalPlan.count; }

    int32_t getBaseCount(const version_t &version) const;

    int32_t getNodeCount() const { return _replicaNodes.size(); }

    void getResourceTags(std::set<tag_t> *tags) const;

    bool isVersionExist(const version_t &ver) const {
        return _versionedPlans.find(ver) != _versionedPlans.end();
    }

    void fillRoleStatus(RoleStatus *status) const;

    void removeVersionPlan(version_t version);

    roleguid_t getGUID() const { return _roleGUID; }

    bool recoverServiceAdapters();

    void releaseSlot(const ReleaseSlotInfo &releaseSlotInfo);

    static bool getSmoothRecoverFlag(const GlobalPlan &globalPlan);
    
public: // only for test
    void setReplicaNodes(const ReplicaNodeVect &replicaNodes) {
        _replicaNodes = replicaNodes;
    }

    void setGlobalPlan(const GlobalPlan &globalPlan) {
        _globalPlan = globalPlan;
    }

    void setVersionedPlan(const ExtVersionedPlanMap &versionedPlan) {
        _versionedPlans = versionedPlan;
    }

    void setHealthChecker(const HealthCheckerPtr &healthCheckerPtr) {
        _healthCheckerPtr = healthCheckerPtr;
    }

    void setServiceSwitch(const ServiceSwitchPtr &serviceSwitchPtr) {
        _serviceSwitchPtr = serviceSwitchPtr;
    }

private:
    void assignSlots();
    
    void ajustReplicaNodes();
    
    void scheduleAllReplicaNodes();
    
    void setFinalPlan();
    
    void updateServiceConfigs();
    
    void updateSlotInfos(const WorkerNodeVect &workerNodes);

    void updateHealthAndServiceInfos(const WorkerNodeVect &workerNodes);
    
    void updateHealthInfos(const HealthCheckerPtr &healthCheckerPtr,
                           const WorkerNodeVect &workerNodes);
    
    void updateServiceInfos(const ServiceSwitchPtr &serviceSwitchPtr,
                            const WorkerNodeVect &workerNodes);
    
    int32_t getTargetLatestCount() const;

    version_t selectOldVersion() const;

    ReplicaNodePtr createReplicaNode();

    void getAllWorkerNodes(WorkerNodeVect *workerNodes) const;

    bool recoverReplicaNodes();

    void updateBrokenRecoverQuotaConfig();

    void getVersionedPlan(VersionedPlanMap *versionedPlanMap) const;

    bool isWorking();

    //virtual for test
    virtual HealthCheckerPtr getHealthChecker();

    virtual ServiceSwitchPtr getServiceSwitch();

    std::string versionToUserDefVersion(version_t version) const;

    bool isCompleted() const; //the role is ready for target

    bool requireVirtualIP() const;

    std::vector<ServiceConfig> filterServiceConfigs(const std::vector<ServiceConfig>& oldConfigs);


private:
    groupid_t _groupId;
    roleid_t _roleId;
    roleguid_t _roleGUID;
    GlobalPlan _globalPlan;
    ExtVersionedPlanMap _versionedPlans;
    version_t _latestVersion;
    ReplicaNodeVect _replicaNodes;
    
    ScheduleParams _scheduleParams;
    SlotInfoMap _slotInfos;
    HippoAdapterPtr _hippoAdapterPtr;
    HealthCheckerPtr _healthCheckerPtr;
    ServiceSwitchPtr _serviceSwitchPtr;
    BrokenRecoverQuotaPtr _brokenRecoverQuotaPtr;
    HealthCheckerManagerPtr _healthCheckerManagerPtr;
    ServiceSwitchManagerPtr _serviceSwitchManagerPtr;
    RoleExecutorPtr _roleExecutorPtr;
    ReplicaNodeCreatorPtr _replicaNodeCreatorPtr;
    IdGeneratorPtr _idGeneratorPtr;
private:
    CARBON_LOG_DECLARE();
};

CARBON_TYPEDEF_PTR(Role);

typedef std::map<std::string, RolePtr> RoleMap;

END_CARBON_NAMESPACE(master);

#endif //CARBON_ROLE_H
