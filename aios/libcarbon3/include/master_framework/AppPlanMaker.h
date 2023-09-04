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
#ifndef MASTER_FRAMEWORK_APPPLANMAKER_H
#define MASTER_FRAMEWORK_APPPLANMAKER_H

#include "master_framework/common.h"
#include "master_framework/AppPlan.h"

BEGIN_MASTER_FRAMEWORK_NAMESPACE(simple_master);

typedef std::pair<std::string, std::string> GroupRolePair; // <group, role>

class AppPlanMaker
{
public:
    AppPlanMaker() {}
    virtual ~AppPlanMaker() {}
private:
    AppPlanMaker(const AppPlanMaker &);
    AppPlanMaker& operator=(const AppPlanMaker &);
public:
    bool loadConfig(const std::string &configPath) {
        return true;
    }
    AppPlan makeAppPlan(const std::vector<GroupRolePair> &groupRoles) const {
        return AppPlan();
    }
protected:
    virtual bool doLoadConfig(autil::legacy::Jsonizable::JsonWrapper &json) {
        return true;
    }
    virtual bool rewriteRolePlan(const std::string &groupName,
                                 const std::string &roleName,
                                 RolePlan &rolePlan) const
    {
        return true;
    }
private:
    MASTER_FRAMEWORK_LOG_DECLARE();
};

MASTER_FRAMEWORK_TYPEDEF_PTR(AppPlanMaker);

END_MASTER_FRAMEWORK_NAMESPACE(master_framework);

#endif //MASTER_FRAMEWORK_APPPLANMAKER_H
