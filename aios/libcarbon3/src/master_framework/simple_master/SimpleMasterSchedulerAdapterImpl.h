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
#ifndef MASTER_FRAMEWORK_SIMPLEMASTERSCHEDULERADAPTERIMPL_H
#define MASTER_FRAMEWORK_SIMPLEMASTERSCHEDULERADAPTERIMPL_H

#include "master_framework/common.h"
#include "master_framework/AppPlan.h"
#include "master_framework/SimpleMasterSchedulerAdapter.h"
#include "master_framework/ScheduleUnitManager.h"
#include "master_framework/PartitionScheduleUnitFactory.h"
#include "simple_master/SimpleMasterSchedulerOrigin.h"
#include "simple_master/Router.h"
#include "master/GlobalConfigManager.h"
#include "master/SerializerCreator.h"
#include "master/CarbonDriverWrapperImpl.h"

#include "hippo/LeaderSerializer.h"
#include "worker_framework/LeaderChecker.h"
#include "hippo/MasterDriver.h"
#include "autil/LoopThread.h"
#include "carbon/GroupPlan.h"
#include "carbon/Status.h"

BEGIN_MASTER_FRAMEWORK_NAMESPACE(simple_master);

class SimpleMasterSchedulerAdapterImpl : public SimpleMasterSchedulerAdapter 
{
public: 
    SimpleMasterSchedulerAdapterImpl();
    virtual ~SimpleMasterSchedulerAdapterImpl();

private:
    SimpleMasterSchedulerAdapterImpl(const SimpleMasterSchedulerAdapterImpl &);
    SimpleMasterSchedulerAdapterImpl& operator=(const SimpleMasterSchedulerAdapterImpl &);

public: 
    virtual bool init(const std::string &hippoZkRoot,
            worker_framework::LeaderChecker * leaderChecker,
            const std::string &applicationId);
    
    virtual bool start();

    virtual bool stop();

    virtual void setAppPlan(const AppPlan &appPlan);

    virtual bool isLeader();

    virtual std::vector<hippo::SlotInfo> getRoleSlots(
            const std::string &roleName);

    virtual void getAllRoleSlots(
            std::map<std::string, master_base::SlotInfos> &roleSlots);

    virtual void releaseSlots(const std::vector<hippo::SlotId> &slots,
            const hippo::ReleasePreference &pref);

    virtual hippo::LeaderSerializer* createLeaderSerializer(
            const std::string &zookeeperRoot, const std::string &fileName,
            const std::string &backupRoot = "");

        virtual hippo::MasterDriver* getMasterDriver();
    virtual void setMasterDriver(hippo::MasterDriver* masterDriver);

    virtual ::google::protobuf::Service* getService();
    virtual ::google::protobuf::Service* getOpsService();


private:
    void mergeRoleInfos(
        const std::map<std::string, master_base::SlotInfos> &roleSlots,
        std::map<std::string, master_base::SlotInfos> &targetRoleSlots);
    void mergeSlotInfos(
        const master_base::SlotInfos &SlotInfos, 
        master_base::SlotInfos &targetSlotInfos);

    std::string wrapCarbonZk(worker_framework::LeaderChecker *leaderChecker);
private:
    worker_framework::LeaderChecker *_leaderChecker;
    simple_master::SimpleMasterSchedulerOriginPtr _simpleMasterSchedulerOriginPtr;
    simple_master::RouterPtr _routerPtr;
    carbon::master::SerializerCreatorPtr _serializerCreatorPtr;
    
    carbon::master::CarbonDriverWrapperImplPtr _carbonDriverWrapper;
};

MASTER_FRAMEWORK_TYPEDEF_PTR(SimpleMasterSchedulerAdapterImpl);


END_MASTER_FRAMEWORK_NAMESPACE(simple_master);


#endif // MASTER_FRAMEWORK_SIMPLEMASTERSCHEDULERADAPTERIMPL_H
