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
#include "swift/admin/AppPlanMaker.h"

#include <algorithm>
#include <memory>
#include <ostream>
#include <stddef.h>
#include <type_traits>

#include "autil/CommonMacros.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "autil/legacy/any.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/jsonizable.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "fslib/common/common_type.h"
#include "fslib/fs/File.h"
#include "fslib/fs/FileSystem.h"
#include "swift/common/PathDefine.h"
#include "swift/config/AdminConfig.h"
#include "swift/config/ConfigDefine.h"
#include "swift/protocol/Common.pb.h"
#include "swift/util/MultiProcessUtil.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;
using namespace autil::legacy::json;
using namespace swift::config;
using namespace swift::common;
using namespace swift::protocol;
using namespace swift::util;
using namespace fslib::fs;

namespace swift {
namespace admin {
AUTIL_LOG_SETUP(swift, AppPlanMaker);

const string AppPlanMaker::SWIFT_WORKER_NAME = "swift_broker";
const string AppPlanMaker::SWIFT_LOADER_NAME = "swift_broker_loader.sh";
const string AppPlanMaker::SWIFT_BROKER_DEFAULT_RESOURCE_GROUP_NAME = "broker";
const string AppPlanMaker::SWIFT_BROKER_ROLE_NAME_PREFIX = "broker_";
const string AppPlanMaker::SWIFT_NET_PRIORITY_PREFIX = "NET_PRIORITY";

AppPlanMaker::AppPlanMaker() : _adminConfig(NULL) {}

AppPlanMaker::~AppPlanMaker() { DELETE_AND_SET_NULL(_adminConfig); }

bool AppPlanMaker::init(const string &configPath, const string &roleVersion) {
    _configPath = configPath;
    _roleVersion = roleVersion;
    AUTIL_LOG(INFO, "make app plan in path : [%s]", _configPath.c_str());
    string hippoConfig = FileSystem::joinFilePath(_configPath, config::HIPPO_FILE_NAME);
    _adminConfig = AdminConfig::loadConfig(configPath);
    if (!_adminConfig) {
        AUTIL_LOG(ERROR, "create admin config fail");
        return false;
    }
    string fileContent;
    if (fslib::EC_OK != FileSystem::readFile(hippoConfig, fileContent)) {
        AUTIL_LOG(ERROR, "Init AppPlanMaker failed : Read file[%s] failed.", hippoConfig.c_str());
        return false;
    }
    AUTIL_LOG(INFO, "hippo config content[%s]", fileContent.c_str());
    try {
        FromJsonString(*this, fileContent);
    } catch (const autil::legacy::ExceptionBase &e) {
        AUTIL_LOG(ERROR,
                  "Invalid json file[%s], content[%s], exception[%s]",
                  _configPath.c_str(),
                  fileContent.c_str(),
                  e.what());
        return false;
    }
    generateRoleName();
    return true;
}

void AppPlanMaker::makeBrokerRolePlan(AppPlan &plan) const {
    vector<GroupRolePair> groupRoles;
    generateGroupRolePairs(groupRoles);
    const auto &groupNetPriorityMap = _adminConfig->getGroupNetPriorityMap();
    for (size_t i = 0; i < groupRoles.size(); ++i) {
        const string &groupName = groupRoles[i].first;
        const string &roleName = groupRoles[i].second;
        RolePlan rolePlan = _defaultRolePlan;

        rolePlan.packageInfos = _packageInfos;
        RolePlanMap::const_iterator it = _customizedRolePlans.find(groupName);
        if (it != _customizedRolePlans.end()) {
            mergeRolePlan(rolePlan, it->second);
        }
        if (rolePlan.processInfos.empty()) {
            rolePlan.processInfos.resize(1);
        }
        rolePlan.count = 1;
        rolePlan.processInfos[0].isDaemon = true;
        rolePlan.processInfos[0].name = SWIFT_WORKER_NAME;
        if (_adminConfig->getPromoteIoThreadPriority()) {
            rolePlan.processInfos[0].cmd = SWIFT_LOADER_NAME;
        } else {
            rolePlan.processInfos[0].cmd = SWIFT_WORKER_NAME;
        }
        rolePlan.processInfos[0].args.push_back(PairType("-w", "${HIPPO_PROC_WORKDIR}"));
        // set args
        vector<PairType> &args = rolePlan.processInfos[0].args;
        args.push_back(PairType("-r", roleName));
        args.push_back(make_pair("-c", _configPath));
        args.push_back(make_pair("-t", StringUtil::toString(_adminConfig->getThreadNum())));
        args.push_back(make_pair("-q", StringUtil::toString(_adminConfig->getQueueSize())));
        args.push_back(make_pair("-i", StringUtil::toString(_adminConfig->getIoThreadNum())));
        if (_adminConfig->getAnetTimeoutLoopIntervalMs() > 0) {
            args.push_back(make_pair("--anetTimeoutLoopIntervalMs",
                                     StringUtil::toString(_adminConfig->getAnetTimeoutLoopIntervalMs())));
        }
        if (_adminConfig->getExclusiveListenThread()) {
            args.push_back(make_pair("", "--exclusiveListenThread"));
        }
        args.push_back(make_pair("", "--recommendPort")); // recommend port
        args.push_back(make_pair("", "--enableANetMetric"));
        args.push_back(make_pair("", "--enableARPCMetric"));
        if (_adminConfig->getHeartbeatIntervalInUs() > 0) {
            args.push_back(
                make_pair("--idleSleepTimeUs", StringUtil::toString(_adminConfig->getHeartbeatIntervalInUs())));
        }
        MultiProcessUtil::generateLogSenderProcess(rolePlan.processInfos, ROLE_TYPE_BROKER);
        // fill role resource
        GroupResourceMap::const_iterator resourceIt = _groupResourceMap.find(groupName);
        if (resourceIt == _groupResourceMap.end()) {
            resourceIt = _groupResourceMap.find(SWIFT_BROKER_DEFAULT_RESOURCE_GROUP_NAME);
            if (resourceIt == _groupResourceMap.end()) {
                AUTIL_LOG(WARN,
                          "group name [%s]'s resource not found in config",
                          SWIFT_BROKER_DEFAULT_RESOURCE_GROUP_NAME.c_str());
                continue;
            }
        }
        hippo::SlotResource slotResource;
        slotResource.resources = resourceIt->second;
        string exclusiveStr;
        string appId = _adminConfig->getApplicationId();
        if (_adminConfig->getExclusiveLevel() == BROKER_EL_ALL) {
            exclusiveStr = appId + "_broker_" + _roleVersion;
        } else if (_adminConfig->getExclusiveLevel() == BROKER_EL_GROUP) {
            exclusiveStr = appId + "_broker_" + groupName + "_" + _roleVersion;
        }
        if (!exclusiveStr.empty()) {
            hippo::Resource resource;
            resource.name = exclusiveStr;
            resource.amount = 1;
            resource.type = hippo::Resource::EXCLUSIVE;
            slotResource.resources.push_back(resource);
        }
        rolePlan.slotResources.push_back(slotResource);
        setGroupNetPriority(groupNetPriorityMap, groupName, rolePlan);
        plan.rolePlans[roleName] = rolePlan;
    }
}

uint32_t AppPlanMaker::getRoleCount(const protocol::RoleType &roleType) const {
    uint32_t count = 0;
    if (roleType == ROLE_TYPE_BROKER) {
        count = _brokerRoleNames.size();
    } else if (roleType == ROLE_TYPE_ALL) {
        count = _brokerRoleNames.size();
    }
    return count;
}

std::vector<std::string> AppPlanMaker::getRoleName(const protocol::RoleType &roleType) const {
    std::vector<std::string> allRole;
    if (roleType == ROLE_TYPE_BROKER) {
        allRole.insert(allRole.end(), _brokerRoleNames.begin(), _brokerRoleNames.end());
    } else if (roleType == ROLE_TYPE_ALL) {
        allRole.insert(allRole.end(), _brokerRoleNames.begin(), _brokerRoleNames.end());
    }
    return allRole;
}

void AppPlanMaker::generateRoleName() {
    ostringstream oss;
    const map<string, uint32_t> &groupMap = _adminConfig->getGroupBrokerCountMap();
    map<string, uint32_t>::const_iterator iter = groupMap.begin();
    for (; iter != groupMap.end(); iter++) {
        for (uint32_t i = 0; i < iter->second; i++) {
            oss << iter->first << "##" << SWIFT_BROKER_ROLE_NAME_PREFIX << i << "_" << getRoleVersion();
            _brokerRoleNames.push_back(oss.str());
            oss.str("");
        }
    }
}

void AppPlanMaker::setGroupNetPriority(const std::map<std::string, uint32_t> &groupNetPriorityMap,
                                       const std::string &groupName,
                                       RolePlan &rolePlan) const {
    auto iter = groupNetPriorityMap.find(groupName);
    if (iter != groupNetPriorityMap.end()) {
        auto &containerConfigs = rolePlan.containerConfigs;
        bool hasNetPriorityConfig = false;
        std::string netPriorityStr = SWIFT_NET_PRIORITY_PREFIX + "=" + StringUtil::toString(iter->second);
        for (auto &config : containerConfigs) {
            if (StringUtil::startsWith(config, SWIFT_NET_PRIORITY_PREFIX)) {
                config = netPriorityStr;
                hasNetPriorityConfig = true;
            }
        }
        if (!hasNetPriorityConfig) {
            containerConfigs.push_back(netPriorityStr);
        }
    }
}

void AppPlanMaker::generateGroupRolePairs(vector<GroupRolePair> &roles) const {
    for (uint32_t i = 0; i < _brokerRoleNames.size(); i++) {
        string group, name;
        PathDefine::parseRoleGroup(_brokerRoleNames[i], group, name);
        roles.push_back(make_pair(group, _brokerRoleNames[i]));
    }
}

string AppPlanMaker::getRoleVersion() const { return _roleVersion; }

string AppPlanMaker::getConfigPath() const { return _configPath; }

void AppPlanMaker::Jsonize(JsonWrapper &json) {
    json.Jsonize("packages", _packageInfos, _packageInfos);
    json.Jsonize("role_default", _defaultRolePlan, _defaultRolePlan);
    json.Jsonize("role_customize", _customizedRolePlans, _customizedRolePlans);
    json.Jsonize("role_resource", _groupResourceMap, _groupResourceMap);
}

void AppPlanMaker::mergeRolePlan(RolePlan &rolePlan, const JsonMap &other) const {
    Any any = ToJson(rolePlan);
    JsonMap &jsonMap = *AnyCast<JsonMap>(&any);
    for (JsonMap::const_iterator it = other.begin(); it != other.end(); it++) {
        jsonMap[it->first] = it->second;
    }
    FromJson(rolePlan, (Any)jsonMap);
}

bool AppPlanMaker::validate() const {
    if (_defaultRolePlan.processInfos.empty()) {
        AUTIL_LOG(ERROR, "processInfo is empty");
        return false;
    }

#define CHECK_RESOURCE(groupName)                                                                                      \
    do {                                                                                                               \
        GroupResourceMap::const_iterator it;                                                                           \
        it = _groupResourceMap.find(groupName);                                                                        \
        if (it == _groupResourceMap.end() || it->second.size() == 0) {                                                 \
            AUTIL_LOG(ERROR, "resource for %s is empty", groupName);                                                   \
            return false;                                                                                              \
        }                                                                                                              \
    } while (0)

    CHECK_RESOURCE("admin");
    CHECK_RESOURCE("broker");
#undef CHECK_RESOURCE
    return true;
}
} // namespace admin
} // namespace swift
