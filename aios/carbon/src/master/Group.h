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
#ifndef CARBON_GROUP_H
#define CARBON_GROUP_H

#include "common/common.h"
#include "carbon/Log.h"
#include "carbon/GroupPlan.h"
#include "carbon/Ops.h"
#include "master/Role.h"
#include "master/RoleCreator.h"
#include "master/Serializer.h"
#include "master/GroupSchedulerCreator.h"
#include "master/HippoAdapter.h"
#include "autil/legacy/jsonizable.h"

BEGIN_CARBON_NAMESPACE(master);

class Group : public autil::legacy::Jsonizable
{
public:
    Group(const groupid_t &groupId,
          const SerializerPtr &serializerPtr,
          const HippoAdapterPtr &hippoAdapterPtr,
          const HealthCheckerManagerPtr &healthCheckerManagerPtr,
          const ServiceSwitchManagerPtr &serviceSwitchManagerPtr);
        
    ~Group();
    
    Group(const Group &group);
    
    Group& operator=(const Group& rhs);

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json);
    
public:
    void setPlan(const version_t &version, const GroupPlan &groupPlan);
    
    void stop();
    
    bool isStopped() const;
    
    void schedule();

    void getResourceTags(std::set<tag_t> *tags) const;

    void fillGroupStatus(GroupStatus *status) const;

    bool recover();

    groupid_t getId() const {
        return _groupId;
    }

public:
    void releaseSlot(const ReleaseSlotInfo &releaseSlotInfo);

private:
    void execute();

    void updateRoles();

    void scheduleRoles();

    void removeStoppedRoles();

    void removeUselessVersions();

    bool persist(const Group &backupGroup);

    bool doPersist();

    void recordPlan(const GroupPlan &groupPlan);
    void recordHistory(const GroupPlan &groupPlan) const ;

    RolePtr createRole(const roleid_t &roleId);
    
    int32_t getGroupAvailablePercent();
public: //for test
    void setCurRoles(const std::map<std::string, RolePtr> &roles) {
        _curRoles = roles;
    }

    void setTargetPlan(const GroupPlan &plan) {
        _targetPlan = plan;
    }

    const RoleMap &getRoles() const {
        return _curRoles;
    }

    void setRoleCreator(const RoleCreatorPtr &roleCreatorPtr) {
        _roleCreatorPtr = roleCreatorPtr;
    }

private:
    bool _hasPlan;
    groupid_t _groupId;
    GroupPlan _targetPlan;
    version_t _curVersion;
    RoleMap _curRoles;
    
    SerializerPtr _serializerPtr;
    RoleCreatorPtr _roleCreatorPtr;
    HippoAdapterPtr _hippoAdapterPtr;
    HealthCheckerManagerPtr _healthCheckerManagerPtr;
    ServiceSwitchManagerPtr _serviceSwitchManagerPtr;
    GroupSchedulerCreator _groupSchedulerCreator;
    std::shared_ptr<rapidjson::StringBuffer> _bufferForLast;
    std::shared_ptr<rapidjson::StringBuffer> _bufferForCur;
private:
    CARBON_LOG_DECLARE();
};

CARBON_TYPEDEF_PTR(Group);

END_CARBON_NAMESPACE(master);

#endif //CARBON_GROUP_H
