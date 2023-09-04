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
#ifndef CARBON_GROUPVERSIONGENERATOR_H
#define CARBON_GROUPVERSIONGENERATOR_H

#include "carbon/Log.h"
#include "common/common.h"
#include "carbon/GroupPlan.h"

BEGIN_CARBON_NAMESPACE(master);

class GroupVersionGenerator
{
public:
    GroupVersionGenerator();
    ~GroupVersionGenerator();
private:
    GroupVersionGenerator(const GroupVersionGenerator &);
    GroupVersionGenerator& operator=(const GroupVersionGenerator &);
public:
    virtual version_t genGroupPlanVersion(const GroupPlan &plan) = 0;
    
private:
CARBON_LOG_DECLARE();
};

class GroupVersionGeneratorV0 : public GroupVersionGenerator {
public:
    version_t genGroupPlanVersion(const GroupPlan &plan) {
        std::string groupSignature = "";
        std::map<roleid_t, RolePlan>::const_iterator it = plan.rolePlans.begin();
        for (; it != plan.rolePlans.end(); it++) {
            groupSignature += it->second.genCheckSum();
        }
        return autil::StringUtil::strToHexStr(
                autil::ExtendHashFunction::hashString(groupSignature));
    }
};

// Generate group version ignoring 'group' in ResourcePlan
class GroupVersionGeneratorV1 : public GroupVersionGenerator {
public:
    version_t genGroupPlanVersion(const GroupPlan &plan) {
        std::map<roleid_t, RolePlan> rolePlans = plan.rolePlans;
        
        std::string groupSignature = "";
        std::map<roleid_t, RolePlan>::iterator it = rolePlans.begin();
        for (; it != rolePlans.end(); it++) {
            // ignore the 'group'
            it->second.version.resourcePlan.group = "";
            groupSignature += it->second.genCheckSum();
        }
        return autil::StringUtil::strToHexStr(
                autil::ExtendHashFunction::hashString(groupSignature));
    }
};

CARBON_TYPEDEF_PTR(GroupVersionGenerator);

END_CARBON_NAMESPACE(master);

#endif //CARBON_GROUPVERSIONGENERATOR_H
