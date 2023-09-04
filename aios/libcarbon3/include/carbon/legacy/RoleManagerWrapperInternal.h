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
#ifndef CARBON_ROLEMANAGERWRAPPERINTERNAL_H
#define CARBON_ROLEMANAGERWRAPPERINTERNAL_H

#include "carbon/proto/Carbon.pb.h"
#include "hippo/ResourceManager.h"
#include "autil/legacy/jsonizable.h"
#include "aios/network/http_arpc/ProtoJsonizer.h"
#include "carbon/legacy/GroupStatusWrapper.h"

namespace carbon {

typedef std::vector<proto::RoleDescription> RoleVec;
typedef std::vector<proto::PublishServiceRequest> ServiceVec;
typedef std::map<std::string, proto::PublishServiceRequest> ServiceMap;

struct GroupDesc : public autil::legacy::Jsonizable {
    RoleVec roles;
    ServiceVec services;
    int32_t stepPercent;

    bool forceUpdate; // forceUpdate mean if group is updating, it will add/del/update roles too. this option will not take effect now
    std::string groupId; // will be auto set by RoleManagerWrapper
    
    GroupDesc() {
        stepPercent = 10;
        forceUpdate = false;
    }
    
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
        if (json.GetMode() == autil::legacy::Jsonizable::TO_JSON) {
            autil::legacy::json::JsonMap roleMap;
            toRoleJsonMap(roles, roleMap);
            json.Jsonize("roles", roleMap);

            autil::legacy::json::JsonMap serviceMap;
            toServiceJsonMap(services, serviceMap);
            json.Jsonize("services", serviceMap);
        } else {
            autil::legacy::json::JsonMap roleMap;            
            json.Jsonize("roles", roleMap, roleMap);
            fromRoleJsonMap(roleMap, roles);

            autil::legacy::json::JsonMap serviceMap;
            json.Jsonize("services", serviceMap, serviceMap);
            fromServiceJsonMap(serviceMap, services);
        }

        json.Jsonize("stepPercent", stepPercent, stepPercent);
        json.Jsonize("forceUpdate", forceUpdate, forceUpdate);
        json.Jsonize("groupId", groupId, groupId); 
    }

    void toRoleJsonMap(const RoleVec &roleVec,
                       autil::legacy::json::JsonMap &roleMap)
    {
        roleMap.clear();
        for (size_t i = 0; i < roleVec.size(); i++) {
            autil::legacy::json::JsonMap jsonMap;
            http_arpc::ProtoJsonizer::toJsonMap(roleVec[i], jsonMap);
            roleMap[roleVec[i].rolename()] = jsonMap;
        }
    }

    void fromRoleJsonMap(const autil::legacy::json::JsonMap &roleMap,
                         RoleVec &roleVec)
    {
        roleVec.clear();
        autil::legacy::json::JsonMap::const_iterator it = roleMap.begin();
        for (; it != roleMap.end(); it++) {
            proto::RoleDescription roleDescription;
            const autil::legacy::json::JsonMap *jsonMap =
                autil::legacy::AnyCast<autil::legacy::json::JsonMap>(&(it->second));
            http_arpc::ProtoJsonizer::fromJsonMap(*jsonMap, &roleDescription);
            roleVec.push_back(roleDescription);
        }
    }

    void toServiceJsonMap(const ServiceVec &serviceVec,
                          autil::legacy::json::JsonMap &serviceMap)
    {
        serviceMap.clear();
        for (size_t i = 0; i < serviceVec.size(); i++) {
            autil::legacy::json::JsonMap jsonMap;
            http_arpc::ProtoJsonizer::toJsonMap(serviceVec[i], jsonMap);
            serviceMap[serviceVec[i].rolename()] = jsonMap;
        }
    }

    void fromServiceJsonMap(const autil::legacy::json::JsonMap &serviceMap,
                            ServiceVec &serviceVec)
    {
        serviceVec.clear();
        autil::legacy::json::JsonMap::const_iterator it = serviceMap.begin();
        for (; it != serviceMap.end(); it++) {
            proto::PublishServiceRequest serviceDescription;
            const autil::legacy::json::JsonMap *jsonMap =
                autil::legacy::AnyCast<autil::legacy::json::JsonMap>(&(it->second));
            http_arpc::ProtoJsonizer::fromJsonMap(*jsonMap, &serviceDescription);
            serviceVec.push_back(serviceDescription);
        }
    }
};

class RoleManagerWrapper
{
public:
    RoleManagerWrapper();
    virtual ~RoleManagerWrapper();
private:
    RoleManagerWrapper(const RoleManagerWrapper &);
    RoleManagerWrapper& operator=(const RoleManagerWrapper &);
public:
    static RoleManagerWrapper* getInstance();
    static void destroyInstance(RoleManagerWrapper *roleManagerWrapper);
    
public:
    virtual bool init(const std::string &appId, const std::string &hippoZkPath,
                      const std::string &amZkPath, bool isNewStart,
                      worker_framework::LeaderElector *leaderElector,
                      uint32_t amonitorPort) = 0;
    virtual bool start() = 0;
    virtual bool stop() = 0;

    virtual bool setGroups(const std::map<std::string, GroupDesc> &groups) = 0;
    virtual void getGroupStatuses(
            const std::vector<std::string> &groupids,
            std::map<std::string, GroupStatusWrapper> &groupStatuses) = 0;
    virtual void getGroupStatuses(
            std::map<std::string, GroupStatusWrapper> &groupStatuses) = 0;

    virtual bool cancelRollingUpdate(const std::string &groupName,
            bool force = false, bool keepNewVersion = false) = 0;

    virtual bool releseSlots(
            const std::vector<hippo::proto::SlotId> &slotIds,
            const hippo::proto::PreferenceDescription &preference) = 0;

    virtual ::google::protobuf::Service* getService() const = 0;
    virtual ::google::protobuf::Service* getOpsService() const = 0;
};

typedef std::shared_ptr<RoleManagerWrapper> RoleManagerWrapperPtr;

}

#endif //CARBON_ROLEMANAGERWRAPPER_H
