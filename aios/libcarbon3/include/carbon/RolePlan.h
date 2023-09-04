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
#ifndef CARBON_ROLEPLAN_H
#define CARBON_ROLEPLAN_H

#include "CommonDefine.h"
#include "autil/legacy/jsonizable.h"
#include "autil/StringUtil.h"
#include "autil/ExtendHashFunction.h"

namespace carbon {

class HealthCheckerConfig : public autil::legacy::Jsonizable {
public:
    JSONIZE() {
        JSON_FIELD(name);
        JSON_FIELD(args);
    }

    std::string name;
    KVMap args;
};

enum ServiceAdapterType {
    ST_NONE = 0,
    ST_CM2 = 1,
    ST_VIP = 2,
    ST_LVS = 3,
    ST_HSF = 4,
    ST_ARMORY = 5,
    ST_SLB = 6,
    ST_ECS_ARMORY = 7,
    ST_VPC_SLB = 8,
    ST_DROGO_LVS = 10, // placeholder for drogo lvs sync
    ST_SKYLINE = 11,
    ST_ECS_SKYLINE = 12, // no virt-ip
};

class ServiceConfig : public autil::legacy::Jsonizable{
public:
    ServiceConfig() {
        masked = false;
        deleteDelay = 0;
    }

    JSONIZE() {
        JSON_FIELD(name);
        JSON_FIELD(type);
        JSON_FIELD(configStr);
        JSON_FIELD(metaStr);
        JSON_FIELD(masked);
        JSON_FIELD(deleteDelay);
    }

    std::string name;
    ServiceAdapterType type;
    std::string configStr;
    std::string metaStr;
    bool masked;
    int64_t deleteDelay;
};

class VersionedServiceConfig : public autil::legacy::Jsonizable{
    // warning: used to generate checksum, do not change
public:
    VersionedServiceConfig(const ServiceConfig &config) {
        name = config.name;
        type = config.type;
        configStr = config.configStr;
    }

    JSONIZE() {
        JSON_FIELD(name);
        JSON_FIELD(type);
        JSON_FIELD(configStr);
    }

    std::string name;
    ServiceAdapterType type;
    std::string configStr;
};

class BrokenRecoverQuotaConfig : public autil::legacy::Jsonizable {
public:
    BrokenRecoverQuotaConfig() {
        maxFailedCount = 5;
        timeWindow = 600;
    } 

    JSONIZE() {
        JSON_FIELD(maxFailedCount);
        JSON_FIELD(timeWindow);
    }

    int32_t maxFailedCount;
    int64_t timeWindow; // sec
};

class GlobalPlan : public autil::legacy::Jsonizable {
public:
    GlobalPlan() {
        count = 0;
        latestVersionRatio = 100;
    }

    JSONIZE() {
        JSON_FIELD(count);
        JSON_FIELD(latestVersionRatio);
        JSON_FIELD(healthCheckerConfig);
        JSON_FIELD(serviceConfigs);
        JSON_FIELD(brokenRecoverQuotaConfig);
        JSON_FIELD(properties);
    }    
    
    int32_t count;
    int32_t latestVersionRatio;
    HealthCheckerConfig healthCheckerConfig;
    std::vector<ServiceConfig> serviceConfigs;
    BrokenRecoverQuotaConfig brokenRecoverQuotaConfig;
    std::map<std::string, std::string> properties;
};

class ResourcePlan : public autil::legacy::Jsonizable {
public:
    ResourcePlan() {
        allocateMode = hippo::AUTO;
        cpusetMode = CpusetMode::UNDEFINE;
    }

    bool operator == (const ResourcePlan &rhs) const {
        return (autil::legacy::ToJsonString(*this, true) == 
                autil::legacy::ToJsonString(rhs, true));
    }

    bool operator != (const ResourcePlan &rhs) const {
        return !(*this == rhs);
    }
    
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
        json.Jsonize("resources", resources, resources);
        json.Jsonize("declarations", declarations, declarations);
        json.Jsonize("queue", queue, queue);        
        json.Jsonize("priority", priority, priority);
        json.Jsonize("group", group, group);
        json.Jsonize("cpusetMode", cpusetMode, cpusetMode);
        json.Jsonize("metaTags", metaTags, metaTags);
        if (json.GetMode() == autil::legacy::Jsonizable::TO_JSON) {
            std::string modeStr = hippo::RoleRequest::convertAllocateMode(
                    allocateMode); 
            json.Jsonize("allocateMode", modeStr);
            if (!containerConfigs.empty()) {
                json.Jsonize("containerConfigs", containerConfigs, containerConfigs);
            }
            if (autil::legacy::ToJsonString(constraints) !=
                autil::legacy::ToJsonString(ConstraintConfig()))
            {
                json.Jsonize("constraints", constraints, constraints);
            }
        } else {
            std::string modeStr;
            json.Jsonize("allocateMode", modeStr, std::string("AUTO"));
            allocateMode = hippo::RoleRequest::convertAllocateMode(modeStr);
            json.Jsonize("containerConfigs", containerConfigs, containerConfigs);
            json.Jsonize("constraints", constraints, constraints);
        }
    }

    std::string getChecksum() const {
        ResourcePlan tmpPlan = *this;
        tmpPlan.group = "";
        std::string jsonStr = autil::legacy::ToJsonString(tmpPlan, true);
        return autil::StringUtil::strToHexStr(
                autil::ExtendHashFunction::hashString(jsonStr));
    }

    std::vector<SlotResource> resources;
    std::vector<Resource> declarations;
    ResourceAllocateMode allocateMode;
    std::string queue;
    Priority priority;
    std::string group;
    CpusetMode cpusetMode;
    ConstraintConfig constraints;
    std::vector<std::string> containerConfigs;
    std::map<std::string, std::string> metaTags;
};

class LaunchPlan : public autil::legacy::Jsonizable {
public:
    bool operator == (const LaunchPlan &rhs) const {
        return (autil::legacy::ToJsonString(*this, true) ==
                autil::legacy::ToJsonString(rhs, true));
    }
        
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
        json.Jsonize("packageInfos", packageInfos, packageInfos);
        json.Jsonize("processInfos", processInfos, processInfos);
        json.Jsonize("dataInfos", dataInfos, dataInfos);
        if (json.GetMode() == autil::legacy::Jsonizable::TO_JSON) {
            if (!podDesc.empty()) {
                json.Jsonize("podDesc", podDesc, podDesc);
            }
        } else {
            json.Jsonize("podDesc", podDesc, podDesc);
        }
    }
    
    std::vector<PackageInfo> packageInfos;
    std::vector<ProcessInfo> processInfos;
    std::vector<DataInfo> dataInfos;
    std::string podDesc;
};

class VersionedPlan : public autil::legacy::Jsonizable {
public:
    VersionedPlan() {
        online = true;
        notMatchTimeout = 900;
        notReadyTimeout = -1;
        updatingGracefully = true;
        restartAfterResourceChange = true;
        preload = false;
    };

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
        JSON_FIELD(resourcePlan);
        JSON_FIELD(launchPlan);
        JSON_FIELD(signature);
        JSON_FIELD(customInfo);
        JSON_FIELD(online);
        JSON_FIELD(userDefVersion);
        JSON_FIELD(notMatchTimeout);
        JSON_FIELD(updatingGracefully);
        JSON_FIELD(preload);
        if (json.GetMode() == autil::legacy::Jsonizable::TO_JSON) {
            if (!restartAfterResourceChange) {
                JSON_FIELD(restartAfterResourceChange);
            }
            if (notReadyTimeout != -1) {
                JSON_FIELD(notReadyTimeout);
            }
        } else {
            JSON_FIELD(restartAfterResourceChange);
            JSON_FIELD(notReadyTimeout);
        }
    }

    std::string genCheckSum() const {
        VersionedPlan tmpPlan;
        tmpPlan.resourcePlan = resourcePlan;
        tmpPlan.launchPlan = launchPlan;
        tmpPlan.signature = signature;
        std::string jsonStr = autil::legacy::ToJsonString(tmpPlan, true);
        return autil::StringUtil::strToHexStr(
                autil::ExtendHashFunction::hashString(jsonStr));
    }

    bool isEmpty() const {
        return resourcePlan == ResourcePlan() &&
            launchPlan == LaunchPlan();
    }

    void updateBoardcastField(const VersionedPlan &rhs) {
        userDefVersion = rhs.userDefVersion;
        customInfo = rhs.customInfo;
        online = rhs.online;
        notMatchTimeout = rhs.notMatchTimeout;
        updatingGracefully = rhs.updatingGracefully;
        restartAfterResourceChange = rhs.restartAfterResourceChange;
    }

private:        
    bool operator == (const VersionedPlan &rhs) const {
        assert(false);
        return false;
    }

public:
    //used when generate version
    ResourcePlan resourcePlan;    
    LaunchPlan launchPlan;
    std::string signature;

    //not used when generate version
    std::string userDefVersion;
    std::string customInfo;
    bool online;
    int32_t notMatchTimeout; // sec
    int32_t notReadyTimeout; // sec
    bool updatingGracefully;
    bool restartAfterResourceChange;
    bool preload;
};

typedef std::map<version_t, VersionedPlan> VersionedPlanMap;

class RolePlan : public autil::legacy::Jsonizable {
public:
    RolePlan(){}

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
        JSON_FIELD(roleId);
        JSON_FIELD(global);
        JSON_FIELD(version);
    }

    std::string genCheckSum() const {
        return version.genCheckSum();
    }
    
    roleid_t roleId;
    GlobalPlan global;
    VersionedPlan version;
};

}

#endif //CARBON_ROLEPLAN_H
