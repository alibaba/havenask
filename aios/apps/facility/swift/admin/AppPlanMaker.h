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
#include <stdint.h>
#include <string>
#include <utility>
#include <vector>

#include "autil/Lock.h"
#include "autil/Log.h"
#include "autil/legacy/json.h"
#include "autil/legacy/jsonizable.h"
#include "hippo/DriverCommon.h"
#include "master_framework/AppPlan.h"
#include "master_framework/RolePlan.h"
#include "master_framework/common.h"
#include "swift/common/Common.h"
#include "swift/protocol/Common.pb.h"

namespace swift {
namespace config {
class AdminConfig;
} // namespace config
} // namespace swift

namespace swift {
namespace admin {
typedef std::pair<std::string, std::string> GroupRolePair; // <group, role>
typedef MF_NS(master_base)::RolePlan RolePlan;
typedef MF_NS(simple_master)::AppPlan AppPlan;
typedef std::pair<std::string, std::string> PairType;

class AppPlanMaker : public autil::legacy::Jsonizable {
public:
    AppPlanMaker();
    virtual ~AppPlanMaker();

private:
    AppPlanMaker(const AppPlanMaker &);
    AppPlanMaker &operator=(const AppPlanMaker &);

public:
    bool init(const std::string &configPath, const std::string &roleVersion);
    void makeBrokerRolePlan(AppPlan &plan) const;
    std::string getRoleVersion() const;
    std::string getConfigPath() const;
    uint32_t getRoleCount(const protocol::RoleType &roleType) const;
    std::vector<std::string> getRoleName(const protocol::RoleType &roleType) const;
    bool validate() const;

public:
    /* override */ void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json);

protected:
    void mergeRolePlan(RolePlan &rolePlan, const autil::legacy::json::JsonMap &other) const;

private:
    void generateGroupRolePairs(std::vector<GroupRolePair> &roles) const;
    void generateRoleName();

private:
    typedef std::map<std::string, autil::legacy::json::JsonMap> RolePlanMap;
    typedef std::map<std::string, std::vector<hippo::Resource>> GroupResourceMap;

private:
    static const std::string SWIFT_WORKER_NAME;
    static const std::string SWIFT_LOADER_NAME;
    static const std::string SWIFT_BROKER_DEFAULT_RESOURCE_GROUP_NAME;
    static const std::string SWIFT_BROKER_ROLE_NAME_PREFIX;

protected:
    std::string _configPath;
    std::string _roleVersion;
    mutable autil::ThreadMutex _configMutex;
    std::vector<hippo::PackageInfo> _packageInfos;
    std::vector<std::string> _brokerRoleNames;
    RolePlan _defaultRolePlan;
    RolePlanMap _customizedRolePlans;
    GroupResourceMap _groupResourceMap;
    config::AdminConfig *_adminConfig;

private:
    AUTIL_LOG_DECLARE();
};

SWIFT_TYPEDEF_PTR(AppPlanMaker);

} // namespace admin
} // namespace swift
