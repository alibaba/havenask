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
#ifndef MASTER_FRAMEWORK_EXTENDEDROLEPLAN_H
#define MASTER_FRAMEWORK_EXTENDEDROLEPLAN_H

#include "master_framework/common.h"
#include "master_framework/RolePlan.h"

BEGIN_MASTER_FRAMEWORK_NAMESPACE(simple_master);

struct ExtendedRolePlan : public RolePlan {
    int32_t targetCount;
    int32_t stepCount;
    int32_t minHealthCapacity;
    simple_master::proto::RoleStatus::Status status;
    std::string roleId;

    ExtendedRolePlan() {
        roleId = "";
        targetCount = -1;
        stepCount = -1;
        minHealthCapacity = -1;
        status = proto::RoleStatus::RS_UNKNOWN;
    }

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
        RolePlan::Jsonize(json);
        json.Jsonize("roleId", roleId, roleId);
        json.Jsonize("stepCount", stepCount, stepCount);
        json.Jsonize("targetCount", targetCount, targetCount);
        json.Jsonize("minHealthCapacity",
                     minHealthCapacity, minHealthCapacity);
        
        if (json.GetMode() == autil::legacy::Jsonizable::TO_JSON) {
            int32_t statusInt = int32_t(status);
            json.Jsonize("status", statusInt);
        } else {
            int32_t statusInt = 0;
            json.Jsonize("status", statusInt, 0);
            status = (proto::RoleStatus::Status) statusInt;
        }
    }
};

END_MASTER_FRAMEWORK_NAMESPACE(simple_master);

#endif //MASTER_FRAMEWORK_EXTENDEDROLEPLAN_H
