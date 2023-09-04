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
#ifndef CARBON_GLOBALCONFIG_H
#define CARBON_GLOBALCONFIG_H

#include "RolePlan.h"
#include "autil/legacy/jsonizable.h"

namespace carbon {

class BufferConfig : public autil::legacy::Jsonizable {
public:
    BufferConfig() : min(2), max(4) {}

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
        JSON_FIELD(min);
        JSON_FIELD(max);
    }

    int min;
    int max;
};

class BufferSlotConfig : public autil::legacy::Jsonizable {
public:
    BufferSlotConfig() {
        autoUpdateLaunchPlan = true;
    }

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
        JSON_FIELD(launchPlan);
        JSON_FIELD(resourcePlan);
        JSON_FIELD(config);
        JSON_FIELD(autoUpdateLaunchPlan);
    }

    bool launchPlanReady() const { return !launchPlan.processInfos.empty(); }
    bool resourcePlanReady() const { return !resourcePlan.resources.empty(); }

    LaunchPlan launchPlan;
    ResourcePlan resourcePlan;
    BufferConfig config;
    bool autoUpdateLaunchPlan;
};

JSONIZABLE_CLASS(RouterRatioModel) {
public:
     RouterRatioModel() : proxyRatio(-1), locRatio(100) {}

     JSONIZE() {
        JSON_FIELD(group);
        JSON_FIELD(proxyRatio);
        JSON_FIELD(locRatio);
    }
    std::string group;  //支持正则匹配
    int32_t proxyRatio; // [0, 100], -1 disable  role中k8s节点比例. default:0
    int32_t locRatio; // [0, 100], -1 disable  role中carbon2节点的比例. default:100

};

JSONIZABLE_CLASS(RouterConfig) {
public:
    RouterConfig() : proxyRatio(-1), locRatio(100), routeAll(false) {}

    JSONIZE() {
        JSON_FIELD(routeGroups);
        JSON_FIELD(blackGroups);
        JSON_FIELD(proxyRatio);
        JSON_FIELD(locRatio);
        JSON_FIELD(routeAll);
        JSON_FIELD(starRouterList);
    }

    std::vector<std::string> routeGroups;
    std::vector<std::string> blackGroups;
    int32_t proxyRatio; // [0, 100], -1 disable
    int32_t locRatio; // [0, 100], -1 disable
    bool routeAll;
    std::vector<RouterRatioModel> starRouterList;//group纬度的路由规则，starRouterList优先级大于blackGroups,routeGroups,routeAll
};

JSONIZABLE_CLASS(SysConfig) {
public:
    JSONIZE() {
        JSON_FIELD(router);
    }

    RouterConfig router;
};

class GlobalConfig : public autil::legacy::Jsonizable {
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
        JSON_FIELD(bufferConfig);
        JSON_FIELD(sysConfig);
    }

    BufferSlotConfig bufferConfig;
    SysConfig sysConfig;
};

}

#endif
