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
#ifndef MASTER_FRAMEWORK_SIMPLEMASTERSCHEDULER_H
#define MASTER_FRAMEWORK_SIMPLEMASTERSCHEDULER_H

#include "master_framework/common.h"
#include "master_framework/SimpleMasterSchedulerAdapter.h"
#include "master_framework/AppPlan.h"

#include "hippo/LeaderSerializer.h"
#include "worker_framework/LeaderChecker.h"
#include "hippo/MasterDriver.h"

BEGIN_MASTER_FRAMEWORK_NAMESPACE(simple_master);

class SimpleMasterScheduler
{
public:
    static const int32_t INF_SLOT_COUNT = 1000000;
public:
    SimpleMasterScheduler();
    virtual ~SimpleMasterScheduler();
private:
    SimpleMasterScheduler(const SimpleMasterScheduler &);
    SimpleMasterScheduler& operator=(const SimpleMasterScheduler &);
public:
    virtual bool init(const std::string &hippoZkRoot,
                      worker_framework::LeaderChecker * leaderChecker,
                      const std::string &applicationId);

    virtual bool start();
    
    virtual bool stop();

    virtual void setAppPlan(const AppPlan &appPlan);

    virtual bool isLeader();

    virtual bool isConnected() {
        assert(false);
        return false;
    }

    virtual std::vector<hippo::SlotInfo> getRoleSlots(
            const std::string &roleName);

    virtual void getAllRoleSlots(
            std::map<std::string, master_base::SlotInfos> &roleSlots);

    virtual void releaseSlots(const std::vector<hippo::SlotId> &slots,
                              const hippo::ReleasePreference &pref);

    virtual void reAllocRoleSlots(const std::string &roleName) {
        assert(false);
    }

    virtual hippo::LeaderSerializer* createLeaderSerializer(
            const std::string &zookeeperRoot, const std::string &fileName,
            const std::string &backupRoot = "");

    virtual hippo::MasterDriver* getMasterDriver();
    virtual void setMasterDriver(hippo::MasterDriver* masterDriver);

    virtual ::google::protobuf::Service* getService();
    virtual ::google::protobuf::Service* getOpsService();

private:
    SimpleMasterSchedulerAdapterPtr _SimpleMasterSchedulerAdapterPtr;
};

MASTER_FRAMEWORK_TYPEDEF_PTR(SimpleMasterScheduler);

class SimpleMasterSchedulerCreator 
{
public:
    virtual SimpleMasterSchedulerPtr create() {
        return SimpleMasterSchedulerPtr(new SimpleMasterScheduler());
    }
    virtual ~SimpleMasterSchedulerCreator() {}
};

MASTER_FRAMEWORK_TYPEDEF_PTR(SimpleMasterSchedulerCreator);

END_MASTER_FRAMEWORK_NAMESPACE(simple_master);

#endif //MASTER_FRAMEWORK_SIMPLEMASTERSCHEDULER_H
