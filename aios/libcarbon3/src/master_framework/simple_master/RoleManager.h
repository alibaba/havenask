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
#ifndef MASTER_FRAMEWORK_ROLEMANAGER_H
#define MASTER_FRAMEWORK_ROLEMANAGER_H

#include "master_framework/common.h"
#include "simple_master/Role.h"
#include "master_framework/RolePlan.h"

BEGIN_MASTER_FRAMEWORK_NAMESPACE(simple_master);

struct RollingUpdateParam {
    int32_t updateRatio;
    int32_t stepCount;
};

class RoleManager
{
public:
    RoleManager();
    ~RoleManager();
private:
    RoleManager(const RoleManager &);
    RoleManager& operator=(const RoleManager &);
public:
    void updateAppPlan(const AppPlan &appPlan,
                       proto::ErrorInfo *errorInfo);                       

    void startRole(const proto::RoleDescription &roleDescription,
                   proto::ErrorInfo *errorInfo);
    
    void stopRole(const std::string &roleName, proto::ErrorInfo *errorInfo);

    void getRoleStatus(const std::string &roleName,
                       proto::RoleStatus *roleStatus);

    void rollingUpdate(const proto::RoleDescription &targetDescription,
                       const RollingUpdateParam &param,
                       proto::ErrorInfo *errorInfo);

    bool cancelRollingUpdate(const std::string &roleName,
                             proto::ErrorInfo *errorInfo);

    bool recoverRollingUpdateFailed(const std::string &roleName,
                                    proto::ErrorInfo *errorInfo);
    
    void scale(in32_t count, proto::ErrorInfo *errorInfo);

    void schedule(const std::map<std::string, SlotInfos> &roleSlotInfos);

    bool recover();

    AppPlan getAppPlan() const;

private:
    bool doRecoverAppPlan(AppPlan &appPlan);
    
private:
    MASTER_FRAMEWORK_LOG_DECLARE();

private:
    std::map<std::string, Role> _roles;
    
};

MASTER_FRAMEWORK_TYPEDEF_PTR(RoleManager);

END_MASTER_FRAMEWORK_NAMESPACE(simple_master);

#endif //MASTER_FRAMEWORK_ROLEMANAGER_H
