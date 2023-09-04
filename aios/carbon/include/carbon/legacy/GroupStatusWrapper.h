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
#ifndef CARBON_GROUPSTATUSWARPPER_H
#define CARBON_GROUPSTATUSWARPPER_H

#include "carbon/proto/Carbon.pb.h"
#include "autil/legacy/jsonizable.h"
#include "aios/network/http_arpc/ProtoJsonizer.h"

namespace carbon {

struct GroupStatusWrapper : public autil::legacy::Jsonizable {
    enum GroupStatusType {
        RUNNING = 0,
        UPDATING = 1
    };
    std::map<std::string, proto::RoleStatus> roleStatuses;
    GroupStatusType status;

    GroupStatusWrapper() {
        status = RUNNING;
    }

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
        json.Jsonize("groupStatus", status, status);
        if (json.GetMode() == autil::legacy::Jsonizable::TO_JSON) {
            autil::legacy::json::JsonMap roleStatusMap;
            for (std::map<std::string, proto::RoleStatus>::const_iterator it = roleStatuses.begin();
                 it != roleStatuses.end(); it++)
            {
                autil::legacy::json::JsonMap jsonMap;
                http_arpc::ProtoJsonizer::toJsonMap(it->second, jsonMap);
                roleStatusMap[it->first] = jsonMap;
            }
            json.Jsonize("roleStatuses", roleStatusMap);
        } else {
            autil::legacy::json::JsonMap roleStatusMap;
            json.Jsonize("roleStatuses", roleStatusMap, roleStatusMap);
            for (autil::legacy::json::JsonMap::const_iterator it = roleStatusMap.begin();
                 it != roleStatusMap.end(); it++)
            {
                proto::RoleStatus roleStatus;
                const autil::legacy::json::JsonMap *jsonMap = autil::legacy::AnyCast<autil::legacy::json::JsonMap>(&(it->second));
                http_arpc::ProtoJsonizer::fromJsonMap(*jsonMap, &roleStatus);
                roleStatuses[it->first] = roleStatus;
            }
        }
    }
};

}

#endif
