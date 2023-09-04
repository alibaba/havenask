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
#include "simple_master/Router.h"
#include "common/Log.h"
#include <regex>
#include <cmath>


using namespace std;
using namespace autil;
using namespace autil::legacy;

BEGIN_MASTER_FRAMEWORK_NAMESPACE(simple_master);

MASTER_FRAMEWORK_LOG_SETUP(simple_master, Router);

Router::Router(const std::string &app,  carbon::master::GlobalConfigManagerPtr globalConfigPtr) :
    _globalConfigPtr(globalConfigPtr){
}

   //根据originAppPlan.rolePlans->key匹配 router规则，然后push到不同的AppPlan
bool Router::splitAppPlans(const AppPlan &originAppPlan,
            AppPlan &c2AppPlan, AppPlan &mfAppPlan){
    const carbon::master::RouterConfig &routerConfig = getRouterConfig();
    const std::vector<carbon::master::RouterRatioModel> &starRouterList = routerConfig.starRouterList;
    const int32_t defaultTargetRatio = routerConfig.targetRatio; // [0, 100], -1 disable  role中k8s节点比例. default:0
    const int32_t defaultSrcRatio = routerConfig.srcRatio; // [0, 100], -1 disable  role中carbon2节点的比例. default:100
  
    for (auto& kv : originAppPlan.rolePlans) {
        const std::string &roleName = kv.first;
        const master_base::RolePlan &rolePlan = kv.second;
        bool isMatch = false;
        for (auto& routerRatioModel : starRouterList) {
            const std::string &group = routerRatioModel.group;
            //TODO match roleName
            if(isMatchRole(group, roleName)){
                isMatch = true;
                const int32_t targetRatio = routerRatioModel.targetRatio; // [0, 100], -1 disable  role中k8s节点比例. default:0
                const int32_t srcRatio = routerRatioModel.srcRatio; // [0, 100], -1 disable  role中carbon2节点的比例. default:100
                splitRolePlan(c2AppPlan, mfAppPlan, targetRatio, srcRatio, roleName, rolePlan);
                break;
            }
        }
        //未匹配则默认ratio
        if (!isMatch){
            splitRolePlan(c2AppPlan, mfAppPlan, defaultTargetRatio, defaultSrcRatio, roleName, rolePlan);
        }
    }
    if (!c2AppPlan.rolePlans.empty()){
        c2AppPlan.packageInfos = originAppPlan.packageInfos;
        c2AppPlan.prohibitedIps = originAppPlan.prohibitedIps;
    }
    if (!mfAppPlan.rolePlans.empty()){
        mfAppPlan.packageInfos = originAppPlan.packageInfos;
        mfAppPlan.prohibitedIps = originAppPlan.prohibitedIps;
    }
    return false;
}
   
carbon::master::RouterConfig Router::getRouterConfig(){
    const carbon::master::RouterConfig &conf = _globalConfigPtr->getConfig().router;
    return conf;
}

bool Router::isMatchRole(const std::string &regex, const std::string &roleName){
    if (regex == "") {
        return false;
    }
    return std::regex_match(roleName, std::regex(regex));
}

void Router::splitRolePlan(AppPlan &c2AppPlan, AppPlan &mfAppPlan,
        const int32_t targetRatio, const int32_t srcRatio, const std::string &roleName, 
        const master_base::RolePlan &rolePlan){
    if (targetRatio > 0){
        c2AppPlan.rolePlans[roleName] = rolePlan;
        generateRolePlan(c2AppPlan.rolePlans[roleName], targetRatio); 
    }
    if (srcRatio > 0){
        mfAppPlan.rolePlans[roleName] = rolePlan;
        generateRolePlan(mfAppPlan.rolePlans[roleName], srcRatio);
    }
}

void Router::generateRolePlan(master_base::RolePlan &targetRolePlan, const int32_t targetRatio){
    if (targetRatio <=0){
        return;
    }
    int32_t target = (int32_t) ceil(targetRolePlan.count * targetRatio / 100.0);
    targetRolePlan.count = target;
}

END_MASTER_FRAMEWORK_NAMESPACE(simple_master);

