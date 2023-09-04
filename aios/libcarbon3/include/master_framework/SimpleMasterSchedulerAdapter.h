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
#ifndef MASTER_FRAMEWORK_SIMPLEMASTERSCHEDULERADAPTER_H
#define MASTER_FRAMEWORK_SIMPLEMASTERSCHEDULERADAPTER_H

#include "master_framework/common.h"
#include "master_framework/AppPlan.h"

#include "hippo/LeaderSerializer.h"
#include "worker_framework/LeaderChecker.h"
#include "hippo/MasterDriver.h"

BEGIN_MASTER_FRAMEWORK_NAMESPACE(simple_master);

class SimpleMasterSchedulerAdapter 
{
public: 
    static SimpleMasterSchedulerAdapter *createSimpleMasterSchedulerAdapter();

public: 
    virtual ~SimpleMasterSchedulerAdapter() {}

public: 
    virtual bool init(const std::string &hippoZkRoot,
            worker_framework::LeaderChecker * leaderChecker,
            const std::string &applicationId) = 0;
    
    virtual bool start() = 0;

    virtual bool stop() = 0;

    virtual void setAppPlan(const AppPlan &appPlan) = 0;

    virtual bool isLeader() = 0;

    virtual std::vector<hippo::SlotInfo> getRoleSlots(
            const std::string &roleName) = 0;

    virtual void getAllRoleSlots(
            std::map<std::string, master_base::SlotInfos> &roleSlots) = 0;

    virtual void releaseSlots(const std::vector<hippo::SlotId> &slots,
            const hippo::ReleasePreference &pref) = 0;

    virtual hippo::LeaderSerializer* createLeaderSerializer(
            const std::string &zookeeperRoot, const std::string &fileName,
            const std::string &backupRoot = "") = 0;

        virtual hippo::MasterDriver* getMasterDriver() = 0;
    virtual void setMasterDriver(hippo::MasterDriver* masterDriver) = 0;

    virtual ::google::protobuf::Service* getService() = 0;
    virtual ::google::protobuf::Service* getOpsService() = 0;
};

MASTER_FRAMEWORK_TYPEDEF_PTR(SimpleMasterSchedulerAdapter);

END_MASTER_FRAMEWORK_NAMESPACE(simple_master);

#endif // MASTER_FRAMEWORK_SIMPLEMASTERSCHEDULERADAPTER_H