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
#ifndef MASTER_FRAMEWORK_SIMPLEMASTER_H
#define MASTER_FRAMEWORK_SIMPLEMASTER_H

#include "master_framework/common.h"
#include "master_framework/AppPlan.h"
#include "master_framework/MasterBase.h"
#include "master_framework/SimpleMasterScheduler.h"
#include "master_framework/proto/SimpleMaster.pb.h"
#include "autil/Lock.h"

BEGIN_MASTER_FRAMEWORK_NAMESPACE(simple_master);

class SimpleMasterServiceImpl;
class SimpleMaster : public master_base::MasterBase {
public:
    SimpleMaster();
    virtual ~SimpleMaster();

private:    
    SimpleMaster(const SimpleMaster &);
    SimpleMaster& operator=(const SimpleMaster &);

public:
    void updateAppPlan(const proto::UpdateAppPlanRequest *request,
                       proto::UpdateAppPlanResponse *response);

    void updateRoleProperties(const proto::UpdateRolePropertiesRequest *request,
                              proto::UpdateRolePropertiesResponse *response);

    std::vector<hippo::SlotInfo> getRoleSlots(const std::string &roleName);

    void getAllRoleSlots(std::map<std::string, master_base::SlotInfos> &roleSlots);
    
    void releaseSlots(const std::vector<hippo::SlotId> &slots,
                      const hippo::ReleasePreference &pref);

    hippo::MasterDriver* getMasterDriver() {
        return _simpleMasterSchedulerPtr->getMasterDriver();
    }
    
protected:
    /* override */ bool doInit();
    
    /* override */ bool doPrepareToRun();

    /* override */ void doIdle();

    virtual void doSchedule(
            const std::map<std::string, SlotInfos> &roleSlotInfos,
            AppPlan &appPlan)
    { }
    
protected:
    void getRoleIps(const std::map<std::string, SlotInfos> &roleSlotInfos,
                    const std::string &roleName,
                    std::vector<std::string> &ips);
    
    std::string getIpFromSpec(const std::string &spec);

protected: // protected for test
    void schedule();
    
    bool startSimpleMaster();
    
    bool initLeaderSerializer();
    
    bool recover();
    
    bool doRecoverAppPlan(AppPlan &appPlan);
    
    bool doUpdateAppPlan(const AppPlan &appPlan);

    void updateProperties(const std::string &op,
                          const std::map<std::string, std::string> &kvMap,
                          master_base::RolePlan &rolePlan);

    void setProperties(const std::map<std::string, std::string> &kvMap,
                       master_base::RolePlan &rolePlan);
        
    void delProperties(const std::map<std::string, std::string> &kvMap,
                       master_base::RolePlan &rolePlan);

public: // for test
    void setSimpleMasterSchedulerCreator(
            const SimpleMasterSchedulerCreatorPtr &creator)
    {
        _simpleMasterSchedulerCreatorPtr = creator;
    }

    void setLeaderSerializer(hippo::LeaderSerializer* leaderSerializer) {
        delete _leaderSerializer;
        _leaderSerializer = leaderSerializer;
    }

    AppPlan getAppPlan() {
        return _appPlan;
    }

    void setMasterDriver(hippo::MasterDriver* masterDriver) {
        _simpleMasterSchedulerPtr->setMasterDriver(masterDriver);
    }
    
private:
    SimpleMasterServiceImpl *_simpleMasterServiceImpl;
    hippo::LeaderSerializer *_leaderSerializer;
    std::string _serializePath;
    SimpleMasterSchedulerPtr _simpleMasterSchedulerPtr;
    SimpleMasterSchedulerCreatorPtr _simpleMasterSchedulerCreatorPtr;
    AppPlan _appPlan;
    autil::ThreadMutex _lock;
};

MASTER_FRAMEWORK_TYPEDEF_PTR(SimpleMaster);

END_MASTER_FRAMEWORK_NAMESPACE(simple_master);

#endif //MASTER_FRAMEWORK_SIMPLEMASTER_H
