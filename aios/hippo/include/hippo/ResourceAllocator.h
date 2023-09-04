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
#ifndef HIPPO_RESOURCEALLOCATOR_H
#define HIPPO_RESOURCEALLOCATOR_H

#include "hippo/ScheduleUnit.h"

namespace hippo {

typedef std::vector<SlotResource> SlotResources;
typedef std::vector<Resource> DeclareResources;

class ResourceAllocator
{
protected:
    ResourceAllocator(){}
public:
    virtual ~ResourceAllocator(){}
    // reset shared_ptr after release
    virtual void release(const ReleasePreference *releasePreference = NULL) =0;
private:
    ResourceAllocator(const ResourceAllocator &);
    ResourceAllocator& operator=(const ResourceAllocator &);
public:
    // business interface
    virtual ScheduleUnitPtr allocate(const std::string &name) = 0;
    virtual void deleteScheduleUnit(const std::string &name) = 0;
    virtual void updateResourceRequirement(
            const std::string &queue,
            const groupid_t &groupId,
            const SlotResources &slotResources,
            const DeclareResources &delcareResources,
            const ResourceAllocateMode &allocateMode,
            const Priority priority,
            CpusetMode cpusetMode = CpusetMode::RESERVED) = 0;
    
    virtual void updatePreferenceKeepTime(int32_t time) = 0;
    // not available: reserve defined slot count, 
    virtual void reserve(int32_t reserveCount) = 0;
    virtual int32_t getReserveCount() = 0;
    // don't do resources' schedule
    virtual void freezeResources() = 0;
    // restart resources' schedule
    virtual void unfreezeResources() = 0;

    // get all schedule unit names
    virtual void getAllScheduleUnitNames(
            std::vector<std::string> &scheduleUnitNames) = 0;
};

HIPPO_TYPEDEF_PTR(ResourceAllocator);

} // end of hippo namespace

#endif //HIPPO_RESOURCEALLOCATOR_H
