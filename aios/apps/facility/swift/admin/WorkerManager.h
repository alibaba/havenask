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
#pragma once

#include <deque>
#include <stdint.h>
#include <string>
#include <vector>

#include "autil/Log.h"
#include "autil/LoopThread.h"
#include "master_framework/SimpleMasterScheduler.h"
#include "master_framework/common.h"
#include "swift/admin/RoleInfo.h"
#include "swift/common/Common.h"
#include "swift/protocol/Common.pb.h"
#include "worker_framework/LeaderChecker.h"

namespace master_framework {
namespace simple_master {
class AppPlan;
} // namespace simple_master
} // namespace master_framework

namespace swift {
namespace admin {
class AppPlanMaker;
} // namespace admin
namespace config {
class ConfigVersionManager;
} // namespace config
} // namespace swift

namespace swift {
namespace admin {

class WorkerManager {
public:
    typedef MASTER_FRAMEWORK_NS(simple_master)::SimpleMasterScheduler SimpleMasterScheduler;
    typedef worker_framework::LeaderChecker LeaderChecker;

public:
    WorkerManager();
    virtual ~WorkerManager();

private:
    WorkerManager(const WorkerManager &);
    WorkerManager &operator=(const WorkerManager &);

public:
    virtual bool init(const std::string &hippoRoot, const std::string &applicationId, LeaderChecker *leaderChecker);
    void fillBrokerPlan(const config::ConfigVersionManager &versionManager,
                        MASTER_FRAMEWORK_NS(simple_master)::AppPlan &appPlan);
    void updateAppPlan(const MASTER_FRAMEWORK_NS(simple_master)::AppPlan &appPlan);
    virtual void getReadyRoleCount(const protocol::RoleType roleType,
                                   const std::string &configPath,
                                   const std::string &roleVersion,
                                   uint32_t &total,
                                   uint32_t &ready);
    uint32_t
    getRoleCount(const std::string &configPath, const std::string &roleVersion, const protocol::RoleType &roleType);
    void getRoleNames(const std::string &configPath,
                      const std::string &roleVersion,
                      const protocol::RoleType &roleType,
                      std::vector<std::string> &roleNames);
    virtual void releaseSlotsPref(const std::vector<std::string> &roleNames);

protected:
    AppPlanMaker *getAppPlanMaker(const std::string &configPath, const std::string &roleVersion);

private:
    SimpleMasterScheduler *createSimpleMasterScheduler(const std::string &hippoRoot,
                                                       const std::string &applicationId,
                                                       LeaderChecker *leaderChecker) const;
    RoleInfo transRoleInfo(SlotInfos &slotInfos);
    bool getIp(const std::string &address, std::string &ip);

protected:
    static std::string BROKER_GROUP_NAME;
    static std::string BROKER_ROLE_NAME_PREFIX;

protected:
    SimpleMasterScheduler *_scheduler;

private:
    std::string _applicationId;
    autil::LoopThreadPtr _loopThread;
    std::deque<AppPlanMaker *> _appPlanMakerQueue;
    std::vector<std::string> _roleNames;

private:
    AUTIL_LOG_DECLARE();
};

SWIFT_TYPEDEF_PTR(WorkerManager);

} // namespace admin
} // namespace swift
