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
#ifndef MASTER_FRAMEWORK_ROLE_H
#define MASTER_FRAMEWORK_ROLE_H

#include "master_framework/common.h"
#include "master_framework/RolePlan.h"

BEGIN_MASTER_FRAMEWORK_NAMESPACE(simple_master);

    
class Role
{
public:
    Role(const proto::RoleDescription &roleDescription);
    ~Role();
private:
    Role(const Role &);
    Role& operator=(const Role &);
public:
    bool rollingUpdate(const proto::RoleDescription &targetDescription,
                       const RollingUpdateParam &param,
                       proto::ErrorInfo *errorInfo);
    
    bool cancelRollingUpdate(proto::ErrorInfo *errorInfo);

    bool recoverRollingUpdateFailed(proto::ErrorInfo *errorInfo);

    bool scale(int32_t count, proto::ErrorInfo *errorInfo);

    void schedule(const std::map<std::string, SlotInfos> &roleSlotInfos);

    void getRolePlans(std::map<std::string, RolePlan> *rolePlans);

private:
    master_base::ExtendedRolePlan _curRolePlan;
    master_base::ExtendedRolePlan _nextRolePlan;
    
private:
    MASTER_FRAMEWORK_LOG_DECLARE();
};

MASTER_FRAMEWORK_TYPEDEF_PTR(Role);

END_MASTER_FRAMEWORK_NAMESPACE(simple_master);

#endif //MASTER_FRAMEWORK_ROLE_H
