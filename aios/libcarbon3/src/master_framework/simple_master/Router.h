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
#ifndef MASTER_FRAMEWORK_ROUTER_H
#define MASTER_FRAMEWORK_ROUTER_H

#include "carbon/Log.h"
#include "master_framework/common.h"
#include "master_framework/AppPlan.h"
#include "master/GlobalConfigManager.h"

BEGIN_MASTER_FRAMEWORK_NAMESPACE(simple_master);

class Router
{
public:
    Router(const std::string &app, carbon::master::GlobalConfigManagerPtr globalConfigPtr);

        //根据originAppPlan.rolePlans->key匹配 router规则，然后push到不同的AppPlan
    virtual bool splitAppPlans(const AppPlan &originAppPlan,
            AppPlan& c2AppPlan, AppPlan &mfAppPlan);
             
private:
    carbon::master::RouterConfig getRouterConfig();
    bool isMatchRole(const std::string &regex, const std::string &roleName);
    void splitRolePlan(AppPlan &c2AppPlan, AppPlan &mfAppPlan,
            const int32_t targetRatio, const int32_t srcRatio, const std::string &roleName, 
            const master_base::RolePlan &rolePlan);
    void generateRolePlan(master_base::RolePlan &targetRolePlan, const int32_t targetRatio);

  
private:
    carbon::master::GlobalConfigManagerPtr _globalConfigPtr;//admin灰度配置
};


MASTER_FRAMEWORK_TYPEDEF_PTR(Router);

END_MASTER_FRAMEWORK_NAMESPACE(simple_master);

#endif
