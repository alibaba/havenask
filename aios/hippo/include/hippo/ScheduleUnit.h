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
#ifndef HIPPO_SCHEDULEUNIT_H
#define HIPPO_SCHEDULEUNIT_H

#include "hippo/ScheduleNode.h"

namespace hippo {
class ScheduleUnit;
HIPPO_TYPEDEF_PTR(ScheduleUnit);

class ScheduleUnit
{
protected:
    ScheduleUnit() {}
public:
    virtual ~ScheduleUnit() {}
private:
    ScheduleUnit(const ScheduleUnit &);
    ScheduleUnit& operator=(const ScheduleUnit &);
public:
    // reset shared_ptr after release
    virtual void release(const ReleasePreference *releasePreference = NULL) = 0;
    
    // business => framework
    virtual void setReplicaCount(int32_t count) = 0;
    virtual void setPlan(const PackageInfos &packageInfos,
                         const DataInfos &dataInfos,
                         const ProcessInfos &processInfos) = 0;
    virtual void updatePackageInfos(const PackageInfos &packageInfos) = 0;
    virtual void updateProcessInfos(const ProcessInfos &processInfos) = 0;
    virtual void updateDataInfos(const DataInfos &dataInfos) = 0;
    virtual bool transferScheduleNodesTo(
            ScheduleUnitPtr &targetScheduleUnitPtr,
            std::vector<ScheduleNodePtr> &scheduleNodes) = 0;

    // business <= framework
    virtual int32_t getReplicaCount()  = 0;
    virtual PackageInfos getPackageInfos() = 0;
    virtual ProcessInfos getProcessInfos() = 0;
    virtual DataInfos getDataInfos() = 0;
    virtual std::vector<ScheduleNodePtr> getAllNodes() = 0;
    virtual std::string getName() = 0;
};

} // end of hippo namespace

#endif //HIPPO_SCHEDULEUNIT_H
