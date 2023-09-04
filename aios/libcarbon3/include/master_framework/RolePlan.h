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
#ifndef MASTER_FRAMEWORK_ROLEPLAN_H
#define MASTER_FRAMEWORK_ROLEPLAN_H

#include "master_framework/common.h"
#include "master_framework/Plan.h"
#include "master_framework/proto/SimpleMaster.pb.h"
#include "hippo/DriverCommon.h"

BEGIN_MASTER_FRAMEWORK_NAMESPACE(master_base);

typedef std::vector<hippo::SlotResource> SlotResources;
typedef std::vector<hippo::Resource> DeclareResources;
typedef std::vector<hippo::PackageInfo> PackageInfos;
typedef std::vector<hippo::ProcessInfo> ProcessInfos;
typedef std::vector<hippo::DataInfo> DataInfos;
typedef std::map<std::string, std::string> Properties;
typedef std::vector<hippo::Resource> Resources;
typedef std::vector<hippo::SlotInfo> SlotInfos;
struct RolePlan : public Plan {
    SlotResources slotResources;
    int32_t count;
    hippo::Priority priority;
    PackageInfos packageInfos;
    ProcessInfos processInfos;
    DataInfos dataInfos;
    Properties properties;
    DeclareResources declareResources;
    hippo::MetaTagMap metaTags;
    hippo::ResourceAllocateMode allocateMode;
    std::vector<std::string> containerConfigs;
    std::string queue;
    std::string group;
    RolePlan() {
        count = 0;
        allocateMode = hippo::AUTO;
        queue = "";
        group = "";
    }
    
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
        json.Jsonize("count", count, count);
        json.Jsonize("priority", priority, priority);
        json.Jsonize("properties", properties, properties);
        json.Jsonize("slotResources", slotResources, slotResources);
        json.Jsonize("packageInfos", packageInfos, packageInfos);
        json.Jsonize("processInfos", processInfos, processInfos);
        json.Jsonize("dataInfos", dataInfos, dataInfos);
        json.Jsonize("declareResources", declareResources, declareResources);
        json.Jsonize("metatags", metaTags, metaTags);
        if (json.GetMode() == autil::legacy::Jsonizable::TO_JSON) {
            int32_t modeInt = int32_t(allocateMode);
            json.Jsonize("allocateMode", modeInt);
        } else {
            int32_t modeInt = int32_t(allocateMode);
            json.Jsonize("allocateMode", modeInt, modeInt);
            allocateMode = (hippo::ResourceAllocateMode)modeInt;
        }
        json.Jsonize("queue", queue, queue);
        if (json.GetMode() == autil::legacy::Jsonizable::FROM_JSON) {
            // For compatible reason, you can config `group` by both `group` and `groupId`.
            json.Jsonize("groupId", group, group);
        }
        json.Jsonize("group", group, group);
        json.Jsonize("containerConfigs", containerConfigs, containerConfigs);
    }

    bool getArg(const std::string &procName,
                const std::string &key,
                std::string &value) const
    {
        for (size_t i = 0; i < processInfos.size(); i++) {
            if (processInfos[i].name == procName) {
                const hippo::ProcessInfo &processInfo = processInfos[i];
                for (size_t j = 0; j < processInfo.args.size(); j++) {
                    if (processInfo.args[j].first == key) {
                        value = processInfo.args[j].second;
                        return true;
                    }
                }
            }
        }
        return false;
    }
};

typedef std::map<std::string, RolePlan> RolePlanMap;

END_MASTER_FRAMEWORK_NAMESPACE(master_base);

#endif //MASTER_FRAMEWORK_ROLEPLAN_H
