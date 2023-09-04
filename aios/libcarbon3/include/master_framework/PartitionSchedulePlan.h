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
#ifndef MASTER_FRAMEWORK_PARTITIONSCHEDULEPLAN_H
#define MASTER_FRAMEWORK_PARTITIONSCHEDULEPLAN_H

#include "master_framework/common.h"
#include "master_framework/Plan.h"
#include "master_framework/RolePlan.h"
#include "hippo/DriverCommon.h"

BEGIN_MASTER_FRAMEWORK_NAMESPACE(master_base);


class PartitionSchedulePlan : public Plan
{
public:
    PartitionSchedulePlan();
    ~PartitionSchedulePlan();
public:
    /* override */ bool serialize(std::string &str);
    /* override */ bool deserialize(const std::string &str);

public:
    int32_t _count;  // replica count
    Resources _slotResources;
    ProcessInfos _processInfos;
    DataInfos _dataInfos;
    PackageInfos _packageInfos;
};


MASTER_FRAMEWORK_TYPEDEF_PTR(PartitionSchedulePlan);

END_MASTER_FRAMEWORK_NAMESPACE(master_base);

#endif //MASTER_FRAMEWORK_PARTITIONSCHEDULEPLAN_H
