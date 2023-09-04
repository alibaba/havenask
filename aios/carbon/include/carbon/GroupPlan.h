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
#ifndef CARBON_GROUPPLAN_H
#define CARBON_GROUPPLAN_H

#include "CommonDefine.h"
#include "RolePlan.h"
#include "autil/legacy/jsonizable.h"
#include "autil/StringUtil.h"
#include "autil/ExtendHashFunction.h"


namespace carbon {

class VersionInfo : public autil::legacy::Jsonizable {
public:
    version_t version;
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
        json.Jsonize("version", version, version);
    }    
};

class GroupSchedulerConfig : public autil::legacy::Jsonizable {
public:
    std::string name;
    std::string configStr;
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
        json.Jsonize("name", name, name);
        json.Jsonize("configStr", configStr, configStr);
    }    
};
    
class GroupPlan : public autil::legacy::Jsonizable {
public:
    GroupPlan() {
        minHealthCapacity = 80;
        extraRatio = 10;
    }

    GroupPlan(int32_t minHealthCapacity) {
        this->minHealthCapacity = minHealthCapacity;
    }
    
    void clear() {
        groupId = "";
        rolePlans.clear();
        schedulerConfig.name = "";
        schedulerConfig.configStr = "";
    }
    bool isCleared() const {
        return groupId == "";
    }
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
        json.Jsonize("groupId", groupId, groupId);
        json.Jsonize("minHealthCapacity", minHealthCapacity,
                      minHealthCapacity);
        json.Jsonize("extraRatio", extraRatio, extraRatio);
        json.Jsonize("rolePlans", rolePlans, rolePlans);
        json.Jsonize("schedulerConfig", schedulerConfig, schedulerConfig);
    }
public:
    groupid_t groupId;
    int32_t minHealthCapacity;
    int32_t extraRatio;
    std::map<roleid_t, RolePlan> rolePlans;
    GroupSchedulerConfig schedulerConfig;
};

class VersionedGroupPlan : public autil::legacy::Jsonizable {
public:
    VersionedGroupPlan() {}
    
    VersionedGroupPlan(const GroupPlan &groupPlan) {
        plan = groupPlan;
    }

    version_t getGroupVersion() const {
        return version;
    }

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
        plan.Jsonize(json);
        json.Jsonize("version", version, version);
    }
    
public:
    GroupPlan plan;
    version_t version;
    std::string checksum;
};

typedef std::map<groupid_t, GroupPlan> GroupPlanMap;
typedef std::map<groupid_t, VersionedGroupPlan> VersionedGroupPlanMap;

}

#endif
