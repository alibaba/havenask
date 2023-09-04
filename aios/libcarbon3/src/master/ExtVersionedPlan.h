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
#ifndef CARBON_EXTVERSIONEDPLAN_H
#define CARBON_EXTVERSIONEDPLAN_H

#include "carbon/RolePlan.h"

BEGIN_CARBON_NAMESPACE(master);

class ExtVersionedPlan : public autil::legacy::Jsonizable{
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
        JSON_FIELD(plan);
        JSON_FIELD(availableCountBase);
        JSON_FIELD(resourceTag);
        JSON_FIELD(resourceChecksum);
        JSON_FIELD(processVersion);
    }
public:
    VersionedPlan plan;
    int32_t availableCountBase;
    tag_t resourceTag;
    std::string resourceChecksum;
    std::string processVersion;
};

typedef std::map<version_t, ExtVersionedPlan> ExtVersionedPlanMap;

CARBON_TYPEDEF_PTR(ExtVersionedPlan);

END_CARBON_NAMESPACE(master);

#endif
