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
#ifndef CARBON_GROUPPLANMANAGER_H
#define CARBON_GROUPPLANMANAGER_H

#include "common/common.h"
#include "carbon/Log.h"
#include "carbon/GroupPlan.h"
#include "master/Serializer.h"
#include "master/GroupVersionGenerator.h"

#include "autil/Lock.h"

BEGIN_CARBON_NAMESPACE(master);

class GroupPlanManager
{
public:
    GroupPlanManager(const SerializerPtr &serializerPtr);
    ~GroupPlanManager();
private:
    GroupPlanManager(const GroupPlanManager &);
    GroupPlanManager& operator=(const GroupPlanManager &);
    
public:
    version_t addGroup(const GroupPlan &plan, ErrorInfo *errorInfo);

    version_t updateGroup(const GroupPlan &plan, ErrorInfo *errorInfo);

    void delGroup(const groupid_t &groupId, ErrorInfo *errorInfo);

    void updateAllGroups(const GroupPlanMap &groupPlanMap, ErrorInfo *errorInfo);

    VersionedGroupPlanMap getGroupPlans() const;

public:
    void createPlan(const std::string &path, const std::string &value, ErrorInfo *errorInfo);

    void updatePlan(const std::string &path, const std::string &value, ErrorInfo *errorInfo);
    
    void readPlan(const std::string &path, std::string *value, ErrorInfo *errorInfo);

    void delPlan(const std::string &path, ErrorInfo *errorInfo);
    
public:
    bool recover();
    
private:
    void persist(const VersionedGroupPlanMap &groupPlans, ErrorInfo *errorInfo);

    version_t doAddGroup(const groupid_t &groupId, const GroupPlan &plan,
                         VersionedGroupPlanMap &tmpGroupPlans);
    
    version_t doUpdateGroup(const groupid_t &groupId, const GroupPlan &plan,
                            VersionedGroupPlanMap &tmpGroupPlans);

    void updateAllGroupsLocked(const GroupPlanMap &groupPlanMap,
                               ErrorInfo *errorInfo);

    GroupPlanMap getGroupPlansLocked() const;

private:
    VersionedGroupPlanMap _groupPlans;
    SerializerPtr _serializerPtr;
    GroupVersionGeneratorPtr _versionGeneratorPtr;
    mutable autil::ThreadMutex _mutex;
private:
    CARBON_LOG_DECLARE();
};

CARBON_TYPEDEF_PTR(GroupPlanManager);

END_CARBON_NAMESPACE(master);

#endif //CARBON_GROUPPLANMANAGER_H
