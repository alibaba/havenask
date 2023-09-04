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
#ifndef CARBON_OPS_H
#define CARBON_OPS_H

#include "CommonDefine.h"
namespace carbon {

class CarbonUnitId : public autil::legacy::Jsonizable {
public:
    groupid_t groupId;
    roleid_t roleId;
    int32_t hippoSlotId;
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
        json.Jsonize("groupId", groupId, groupId);
        json.Jsonize("roleId", roleId, roleId);
        json.Jsonize("hippoSlotId", hippoSlotId, hippoSlotId);
    }
    CarbonUnitId(const groupid_t &groupId = "",
                 const roleid_t &roleId = "",
                 const int32_t &hippoSlotId = -1)
    {
        this->groupId = groupId;
        this->roleId = roleId;
        this->hippoSlotId = hippoSlotId;
    }
};
    
class ReleaseSlotInfo : public autil::legacy::Jsonizable {
public:
    CarbonUnitId carbonUnitId;
    uint64_t leaseMs;
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
        json.Jsonize("carbonUnitId", carbonUnitId, carbonUnitId);
        json.Jsonize("leaseMs", leaseMs, leaseMs);
    }
    ReleaseSlotInfo(const CarbonUnitId carbonUnitId = {},
                    const uint64_t &leaseMs = 3600 * 1000)
    {
        this->carbonUnitId = carbonUnitId;
        this->leaseMs = leaseMs;
    }
};

}

#endif
