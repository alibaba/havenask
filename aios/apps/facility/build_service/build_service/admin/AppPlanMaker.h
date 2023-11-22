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

#include <map>
#include <memory>
#include <stddef.h>
#include <string>
#include <utility>
#include <vector>

#include "autil/Lock.h"
#include "autil/legacy/json.h"
#include "autil/legacy/legacy_jsonizable_dec.h"
#include "build_service/admin/MetaTagAppender.h"
#include "build_service/common_define.h"
#include "build_service/proto/BasicDefs.pb.h"
#include "build_service/util/Log.h"
#include "build_service/util/SharedPtrGuard.h"
#include "hippo/DriverCommon.h"
#include "master_framework/AppPlan.h"
#include "master_framework/RolePlan.h"
#include "master_framework/common.h"

BS_DECLARE_REFERENCE_CLASS(proto, WorkerNodeBase);

namespace build_service { namespace admin {
class WorkerTable;

typedef MF_NS(master_base)::RolePlan RolePlan;
typedef MF_NS(simple_master)::AppPlan AppPlan;
typedef std::shared_ptr<AppPlan> AppPlanPtr;
typedef std::pair<std::string, std::string> PairType;

class AppPlanMaker : public autil::legacy::Jsonizable
{
public:
    AppPlanMaker(const std::string& appName, const std::string& heartbeatType);
    virtual ~AppPlanMaker();

private:
    AppPlanMaker& operator=(const AppPlanMaker&);

public:
    void prepare(const WorkerTable* workerTable);
    AppPlanPtr makeAppPlan();
    AppPlanPtr makeAppPlan(const GroupRolePairs& groupRoles);

    virtual bool loadConfig(const std::string& configFile);
    bool validate(const std::vector<std::string>& clusters,
                  const std::map<std::string, std::vector<std::string>>& mergeNames,
                  const std::vector<std::string>& dataTables) const;
    void cleanCache();
    std::shared_ptr<GroupRolePairs> getCandidates();

    std::pair<std::string, RolePlan> createAgentRolePlan(const proto::BuildId& id, const std::string& configName,
                                                         size_t totalNodeCount, size_t idx, bool useDynamicMapping);

    const std::vector<hippo::PackageInfo> getPackageInfos() const { return _packageInfos; }
    void resetWorkerPackageList(const std::vector<hippo::PackageInfo>& packageList);

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;

private:
    bool operator==(const AppPlanMaker& other) const;

private:
    typedef std::vector<autil::legacy::json::JsonMap> RolePlanVector;
    typedef std::map<std::string, std::vector<hippo::Resource>> GroupResourceMap;

private:
    // for test
    void prepareGroupRolePairs(GroupRolePairs& groupRoles);

private:
    bool isWorkerTableUpdated(const WorkerTable* workerTable) const;

    static std::vector<std::string> getAllRoles(const std::vector<std::string>& clusters,
                                                const std::map<std::string, std::vector<std::string>>& mergeNames,
                                                const std::vector<std::string>& dataTables);

    static void mergeRoleResource(const std::string& groupName, const GroupResourceMap& groupResourceMap,
                                  RolePlan& rolePlan);
    static void mergeRoleCustomize(const std::string& groupName, const RolePlanVector& customizedRolePlans,
                                   RolePlan& rolePlan);
    void mergeRoleProcess(RolePlan& rolePlan, const GroupRole& groupRole, const std::string& heartbeatType) const;

    static void mergeRolePlan(RolePlan& rolePlan, const autil::legacy::json::JsonMap& other);
    static std::vector<PairType> argFilter(const std::vector<PairType>& args);
    static GroupRole createGroupRole(proto::WorkerNodeBasePtr node, bool enableGangRegion);
    void CheckAndSetHighQuality(const GroupRole& role, RolePlan& plan);
    void checkAndRewriteRolePlan(RolePlan& plan);
    static void setEnvIfNotExist(RolePlan& rolePlan, const std::string& envKey, const std::string& envValue);

    virtual RolePlan makeRoleForAgent(const std::string& roleName, const std::string& groupName);

private:
    static const std::string BUILD_SERVICE_WORKER_NAME;
    static const std::string ROLE_PATTERN;

protected:
    autil::ThreadMutex _cacheMutex; // locked when access @_rolePlanCache
    std::string _heartbeatType;
    std::vector<hippo::PackageInfo> _packageInfos;
    RolePlan _defaultRolePlan;
    RolePlanVector _customizedRolePlans;
    GroupResourceMap _groupResourceMap;
    util::SharedPtrGuard<GroupRolePairs> _candidateGuard;
    std::map<std::string, RolePlan> _rolePlanCache;
    std::map<std::string, RolePlan> _agentRolePlanCache;
    std::string _appName;
    bool _enableGangRegion;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(AppPlanMaker);

}} // namespace build_service::admin
