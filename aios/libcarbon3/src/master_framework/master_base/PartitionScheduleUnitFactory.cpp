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
#include "master_framework/PartitionScheduleUnitFactory.h"
#include "common/Log.h"

using namespace std;
BEGIN_MASTER_FRAMEWORK_NAMESPACE(master_base);

//MASTER_FRAMEWORK_LOG_SETUP(master_base, PartitionScheduleUnitFactory);

PartitionScheduleUnitFactory::PartitionScheduleUnitFactory() {
}

PartitionScheduleUnitFactory::~PartitionScheduleUnitFactory() {
}

ScheduleUnitPtr PartitionScheduleUnitFactory::createScheduleUnit(
        const std::string &name,
        hippo::MasterDriver *hippoMasterDriver)
{
    ScheduleUnitPtr scheduleUnitPtr(
            new PartitionScheduleUnit(name, hippoMasterDriver));
    return scheduleUnitPtr;
}

END_MASTER_FRAMEWORK_NAMESPACE(master_base);

