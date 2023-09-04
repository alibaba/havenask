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
#ifndef CARBON_BUFFERED_HIPPOADAPTER_H
#define CARBON_BUFFERED_HIPPOADAPTER_H

#include "carbon/CommonDefine.h"
#include "carbon/GlobalConfig.h"
#include "carbon/Status.h"
#include "master/HippoAdapter.h"
#include "master/DiffSerializer.h"
#include "master/SerializerCreator.h"
#include "master/BufferedSlotAllocator.h"
#include "autil/legacy/jsonizable.h"
#include "autil/TimeUtility.h"

BEGIN_CARBON_NAMESPACE(master);

typedef std::set<SlotId> SlotIdSet;

JSONIZABLE_CLASS(VirtualTag)
{
public:
    VirtualTag() : count(0), reqSlotAt(0) {}
    VirtualTag(int32_t c, const tag_t& s) : name(s), count(c) {
        setCount(c);
    }

    JSONIZE() {
        JSON_FIELD(name);
        JSON_FIELD(count);
        JSON_FIELD(slots);
    }

    void setCount(int32_t count) {
        if (this->count != count) 
            this->reqSlotAt = autil::TimeUtility::currentTime();
        this->count = count;
    }
    bool countMatch() const { return count <= (int32_t) slots.size(); }
    void addSlot(hippo::SlotInfo& slot) {
        slot.role = name; // IMPORTANT
        slots.insert(slot.slotId);
    }
    void removeSlot(const hippo::SlotId& id) {
        slots.erase(id);
    }

    tag_t name;
    int32_t count;
    SlotIdSet slots;
    int64_t reqSlotAt;
};

class SlotsBuffer
{
public:
    SlotsBuffer();
    void alloc(SlotInfoMap* dst, VirtualTag* tag, int32_t count);
    bool put(const SlotInfo& slot);
    void erase(const SlotId& id);
    bool recycle(const SlotInfo& slot, ReleasePreference pref);
    int32_t slotSize() const { return (int32_t)_slots.size(); }
    void getAllSlotInfos(SlotInfoVect& slotsVec) const;
    void getAllSlotIds(std::vector<SlotId>* slotIds) const;
    void releaseRedundantSlots(hippo::MasterDriver* masterDriver, int32_t max);
    void setAllocator(allocator::AllocatorPtr alloc) { _allocator = alloc; }
    void getSlotsStatus(BufferSlotStatus* status) const;
    int getHealthRatio() const;

private:
    SlotInfoMap _slots;
    allocator::AllocatorPtr _allocator;
    CARBON_LOG_DECLARE();
};

class SlotPlanMatcher
{
public:
    void setSlotPlan(const SlotId& id, const LaunchPlan& plan);
    void removeSlot(const SlotId& id);
    bool isAllMatch(size_t totalSize);
    std::map<int64_t, size_t> getSigStatus() const;

    std::map<SlotId, int64_t> _slotSigs;
};

typedef std::vector<std::pair<std::vector<hippo::SlotInfo>, hippo::ProcessContext>> SlotsLaunchPlanVect;

class BufferedHippoAdapter : public HippoAdapter
{
public:
    BufferedHippoAdapter(SerializerCreatorPtr serializerCreatorPtr);

    virtual bool start();
    virtual bool recover();

    virtual void allocateSlots(
            const std::map<tag_t, ResourceRequest> &resourcePlans,
            const std::map<SlotId, ReleasePreference> &releasedSlots);

    virtual void launchSlots(const std::map<SlotId, LaunchPlan> &launchPlans,
                             const std::map<SlotId, LaunchPlan> &finalLaunchPlans);

    virtual SlotInfoMap getSlotsByTags(const std::set<tag_t> &tags) const;

    virtual SlotInfoMap getSlotsByTag(const tag_t &tag) const;

    virtual void getAllTags(std::set<tag_t> *tags) const;
    
    virtual bool isWorking() const;

    virtual void releaseTag(const tag_t &tag);

    virtual int64_t getAppChecksum() const;

    virtual void preUpdate();
    virtual void postUpdate();

    void sweepTagSlots(const std::vector<tag_t>& tags);

    void setConfig(BufferSlotConfig config, ErrorInfo* errorInfo, bool lock = true);
    BufferSlotConfig getConfig();

    BufferSlotStatus getStatus() const;

private:
    void getSlotInfos(SlotInfoMap& slotInfos, const SlotIdSet& slotIds, const tag_t& tag) const;
    void releaseSlot(const SlotInfo& slot, ReleasePreference pref);
    void getAllSlots(SlotInfoMap* slots);
    void persist();
    void initTagSlotIds();
    bool recoverSlotInfos();
    bool persistConfig(const BufferSlotConfig& config);
    bool persistTagSlots();
    void updateConfigLaunchPlan(const LaunchPlan& plan);
    void updateConfigResourcePlan(const ResourcePlan& plan);
    void fixLaunchPlan(LaunchPlan& plan);
    void setMatcherSlotPlan(const SlotId& id, const LaunchPlan& plan);
    void doSetConfig(const BufferSlotConfig& config, ErrorInfo* errorInfo, bool lock);
    bool isSlotRecyclable(const SlotInfo& slot);
    void commitSlotsPlans(SlotsLaunchPlanVect* slotsLaunchPlans);
    void commitBufferSlotsPlans(SlotsLaunchPlanVect* slotsLaunchPlans);

UNITTEST_START(public)
    std::map<tag_t, VirtualTag>& getTags() { return _tags; }
    void setHippoDriver(hippo::MasterDriver* driver) { _masterDriver = driver; }
    const SlotsBuffer& getBuffer() { return _buffer; }

    void updateSlots();
    void updateTags(const std::map<tag_t, ResourceRequest> &resourcePlans);
    void doAlloc(const std::map<tag_t, ResourceRequest> &resourcePlans);
    void releaseSlots(const std::map<SlotId, ReleasePreference> &releasedSlots);
    void setupRoleRequest(hippo::RoleRequest* request);
UNITTEST_END()

private:
    std::map<tag_t, VirtualTag> _tags;     
    std::map<SlotId, tag_t> _tagSlotIds;
    SlotInfoMap _slots;
    SlotsBuffer _buffer;
    BufferSlotConfig _config;
    DiffSerializerPtr _tagSlotsSerializer;
    DiffSerializerPtr _configSerializer;
    SlotPlanMatcher _slotPlanMatcher;
    mutable autil::ReadWriteLock _rwLock;
    std::map<SlotId, LaunchPlan> _slotsPlans;
private:
    CARBON_LOG_DECLARE();
};

CARBON_TYPEDEF_PTR(BufferedHippoAdapter);

END_CARBON_NAMESPACE(master);

#endif // CARBON_BUFFERED_HIPPOADAPTER_H
