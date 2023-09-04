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
#ifndef CARBON_BUFFERED_SLOTALLOCATOR_H
#define CARBON_BUFFERED_SLOTALLOCATOR_H

#include "carbon/CommonDefine.h"
#include "common/common.h"

BEGIN_CARBON_NAMESPACE(master);

class VirtualTag;
class SlotsBuffer;

namespace allocator {

class Allocator {
public:
    Allocator(SlotsBuffer* buffer) : _buffer(buffer) {}
    virtual ~Allocator() {}
    virtual void alloc(SlotInfoMap* dst, VirtualTag* tag, int32_t count);
    int scoreSlotBasic(const SlotInfo& slot);
    void sortBasic(SlotInfoVect& slots);

protected:
    SlotsBuffer* _buffer;
};

CARBON_TYPEDEF_PTR(Allocator);

// scattered by hippo slave
class ScatterAllocator : public Allocator {
    typedef std::map<std::string, int> SlaveNodeCountMap;
public:
    ScatterAllocator(SlotsBuffer* buffer);
    virtual void alloc(SlotInfoMap* dst, VirtualTag* tag, int32_t count);

private:
    SlaveNodeCountMap buildNodeCountMap(const VirtualTag* tag);
    int scoreSlot(const SlaveNodeCountMap& countMap, const SlotInfo& slot);
};

}

END_CARBON_NAMESPACE(master);

#endif // CARBON_BUFFERED_SLOTALLOCATOR_H

