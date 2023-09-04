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
#ifndef MASTER_FRAMEWORK_APPPLAN_H
#define MASTER_FRAMEWORK_APPPLAN_H

#include "master_framework/common.h"
#include "master_framework/RolePlan.h"
#include "autil/legacy/jsonizable.h"
#include "hippo/DriverCommon.h"

BEGIN_MASTER_FRAMEWORK_NAMESPACE(simple_master);

class AppPlan : public autil::legacy::Jsonizable
{
public:
    AppPlan();
    ~AppPlan();
public:
    /* override */
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json);
    
public:
    bool fromString(const std::string &content);
    bool operator == (const AppPlan &rh) const;
    
public:
    std::vector<hippo::PackageInfo> packageInfos;
    std::map<std::string, master_base::RolePlan> rolePlans;
    std::vector<std::string> prohibitedIps;
};

MASTER_FRAMEWORK_TYPEDEF_PTR(AppPlan);

END_MASTER_FRAMEWORK_NAMESPACE(simple_master);

#endif //MASTER_FRAMEWORK_APPPLAN_H
