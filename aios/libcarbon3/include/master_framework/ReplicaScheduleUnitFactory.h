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
#ifndef MASTER_FRAMEWORK_REPLICASCHEDULEUNITFACTORY_H
#define MASTER_FRAMEWORK_REPLICASCHEDULEUNITFACTORY_H

#include "master_framework/common.h"

BEGIN_MASTER_FRAMEWORK_NAMESPACE(master_base);

class ReplicaScheduleUnitFactory
{
public:
    ReplicaScheduleUnitFactory();
    ~ReplicaScheduleUnitFactory();
private:
    ReplicaScheduleUnitFactory(const ReplicaScheduleUnitFactory &);
    ReplicaScheduleUnitFactory& operator=(const ReplicaScheduleUnitFactory &);
};

MASTER_FRAMEWORK_TYPEDEF_PTR(ReplicaScheduleUnitFactory);

END_MASTER_FRAMEWORK_NAMESPACE(master_base);

#endif //MASTER_FRAMEWORK_REPLICASCHEDULEUNITFACTORY_H
