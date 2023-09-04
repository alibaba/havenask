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
#include "master/BufferedHippoAdapter.h"
#include "master/SlotUtil.h"
#include "monitor/CarbonMonitorSingleton.h"
#include "fslib/fslib.h"
#include "autil/HashAlgorithm.h"
#include "autil/EnvUtil.h"

BEGIN_CARBON_NAMESPACE(master);

using namespace hippo;
using namespace std;
using namespace autil;
using namespace autil::legacy;
using namespace allocator;

#define TAG_POOL_NAME "_tag_pool_"

CARBON_LOG_SETUP(master, SlotsBuffer);
CARBON_LOG_SETUP(master, BufferedHippoAdapter);

#define COMPRESS true    

SlotsBuffer::SlotsBuffer() {
}

void SlotsBuffer::alloc(SlotInfoMap* dst, VirtualTag* tag, int32_t count) {
    _allocator->alloc(dst, tag, count);
    for (const auto& kv : *dst) {
        _slots.erase(kv.first);
    }
}

bool SlotsBuffer::put(const SlotInfo& slot) {
    auto it = _slots.find(slot.slotId);
    if (it != _slots.end()) {
        it->second = slot;
    } else {
        _slots[slot.slotId] = slot;
    }
    return it == _slots.end();
}

void SlotsBuffer::erase(const SlotId& id) {
    _slots.erase(id);
}

bool SlotsBuffer::recycle(const SlotInfo& slot, ReleasePreference pref) {
    _slots[slot.slotId] = slot;
    return true;
}

void SlotsBuffer::getAllSlotInfos(SlotInfoVect& slotsVec) const {
    for (const auto& kv : _slots) {
        slotsVec.push_back(kv.second);
    }
}

void SlotsBuffer::getAllSlotIds(std::vector<SlotId>* slotIds) const {
    for (const auto& kv : _slots) {
        slotIds->push_back(kv.first);
    }
}

void SlotsBuffer::getSlotsStatus(BufferSlotStatus* status) const {
    for (const auto& kv : _slots) {
        SlotStatus s = SlotUtil::extractSlotStatus(kv.second);
        s.slotId = kv.first;
        status->bufferSlots.push_back(s);
    }
}

int SlotsBuffer::getHealthRatio() const {
    int32_t healthCount = 0;
    for (const auto& kv : _slots) {
        SlotStatus s = SlotUtil::extractSlotStatus(kv.second);
        if (s.status == ST_PROC_RUNNING) {
            healthCount ++;
        }
    }
    return _slots.empty() ? 100 : healthCount * 100 / _slots.size();
}

void SlotsBuffer::releaseRedundantSlots(hippo::MasterDriver* masterDriver, int32_t max) {
    if (slotSize() <= max) return ;
    int32_t count = slotSize() - max;
    CARBON_LOG(INFO, "release buffer redundant slots, count %d", count);
    ReleasePreference pref;
    SlotInfoVect sortedSlots;
    getAllSlotInfos(sortedSlots);
    _allocator->sortBasic(sortedSlots);
    for (auto it = sortedSlots.begin(); it != sortedSlots.end() && count > 0; -- count, ++it) {
        auto& slot = *it;
        if (!SlotUtil::isSlotUnRecoverable(slot)) {
            pref.type = ReleasePreference::RELEASE_PREF_PREFER;
            pref.leaseMs = PREF_PREFER_LEASE_TIME;
        } else {
            pref.type = ReleasePreference::RELEASE_PREF_PROHIBIT;
        }
        CARBON_LOG(INFO, "release buffer redundant slot [%s] pref %d", SlotUtil::toString(slot.slotId).c_str(), pref.type);
        masterDriver->releaseSlot(slot.slotId, pref);
        _slots.erase(slot.slotId);
    }
}

void SlotPlanMatcher::setSlotPlan(const SlotId& id, const LaunchPlan& plan) {
    std::string s = ToJsonString(plan, true);
    int64_t sig = HashAlgorithm::hashString64(s.c_str());
    _slotSigs[id] = sig;
}

void SlotPlanMatcher::removeSlot(const SlotId& id) {
    _slotSigs.erase(id);
}

bool SlotPlanMatcher::isAllMatch(size_t totalSize) {
    if (_slotSigs.size() < totalSize) return false;
    std::set<int64_t> sigs;
    for (const auto& kv : _slotSigs) {
        sigs.insert(kv.second);
    }
    return sigs.size() == 1; 
}

std::map<int64_t, size_t> SlotPlanMatcher::getSigStatus() const {
    std::map<int64_t, size_t> sigSlotCnts;
    for (const auto& kv : _slotSigs) {
        sigSlotCnts[kv.second] += 1;
    }
    return sigSlotCnts;
}

BufferedHippoAdapter::BufferedHippoAdapter(SerializerCreatorPtr scp) : _buffer() {
    _tagSlotsSerializer.reset(new DiffSerializer(scp->create(BUFFERED_TAGSLOTS_FILE_NAME, "", COMPRESS)));
    _configSerializer.reset(new DiffSerializer(scp->create(BUFFERED_CONFIG_FILE_NAME, "", COMPRESS)));
    string allocType = autil::EnvUtil::getEnv("CARBON_ALLOC_POLICY");
    AllocatorPtr allocator;
    if (allocType == "scatter") {
        allocator.reset(new ScatterAllocator(&_buffer));
        CARBON_LOG(INFO, "set buffer allocator type: %s", allocType.c_str());
    } else {
        allocator.reset(new Allocator(&_buffer));
        CARBON_LOG(INFO, "set buffer allocator type: default");
    }
    _buffer.setAllocator(allocator);
}

void BufferedHippoAdapter::allocateSlots(
        const std::map<tag_t, ResourceRequest> &resourcePlans,
        const std::map<SlotId, ReleasePreference> &releasedSlots) {
    if (resourcePlans.empty()) return; 
    // write
    {
        ScopedWriteLock lock(_rwLock);
        releaseSlots(releasedSlots);
        updateTags(resourcePlans);
        doAlloc(resourcePlans);
        if (!_config.resourcePlanReady()) {
            CARBON_LOG(WARN, "not set buffer resource plan, use 1st commited role");
            auto it = resourcePlans.begin();
            updateConfigResourcePlan(it->second.resourcePlan);
        }
    }
    // read
    {
        ScopedReadLock lock(_rwLock);
        RoleRequest roleRequest;
        setupRoleRequest(&roleRequest);
        CARBON_LOG(DEBUG, "send role request to hippo with count [%d]", roleRequest.count);
        PTR_TEST_CALL(_masterDriver, updateRoleRequest(TAG_POOL_NAME, roleRequest));
        persistTagSlots();
    }
}

SlotInfoMap BufferedHippoAdapter::getSlotsByTag(const tag_t &tag) const {
    ScopedReadLock lock(_rwLock);
    SlotInfoMap slotInfos;
    auto it = _tags.find(tag);
    if (it != _tags.end()) {
        getSlotInfos(slotInfos, it->second.slots, tag.c_str());
    }
    return slotInfos;
}

SlotInfoMap BufferedHippoAdapter::getSlotsByTags(const std::set<tag_t> &tags) const {
    ScopedReadLock lock(_rwLock);
    SlotInfoMap slotInfos;
    for (auto it = tags.begin(); it != tags.end(); ++it) {
        auto tit = _tags.find(*it);
        if (tit != _tags.end()) {
            getSlotInfos(slotInfos, tit->second.slots, (*it).c_str());
        }
    }
    return slotInfos;
}

void BufferedHippoAdapter::getSlotInfos(SlotInfoMap& slotInfos, const SlotIdSet& slotIds, const tag_t& tag) const {
    for (auto it = slotIds.begin(); it != slotIds.end(); ++it) {
        auto sit = _slots.find(*it);        
        if (sit != _slots.end()) {
            slotInfos[*it] = sit->second;
        } else {
            CARBON_LOG(WARN, "not found slot info for [%s] in vtag [%s]", SlotUtil::toString(*it).c_str(), tag.c_str());
        }
    }
}

void BufferedHippoAdapter::commitBufferSlotsPlans(SlotsLaunchPlanVect* slotsLaunchPlans) {
    if (_config.launchPlanReady()) {
        SlotInfoVect slotsVec;
        _buffer.getAllSlotInfos(slotsVec);
        if (!slotsVec.empty()) {
            ProcessContext context;
            fillProcessContext(_config.launchPlan, _config.launchPlan, &context);
            slotsLaunchPlans->push_back(std::make_pair(slotsVec, context));
        }
    }
}

// see issue xxxx://invalid/drogo/fiber/issues/47
// If not prepare, hippo SDK will send empty launch plan to some slots if these slots
// are not set by role scheduler in carbon in time.
void BufferedHippoAdapter::commitSlotsPlans(SlotsLaunchPlanVect* slotsLaunchPlans) {
    if (_slotsPlans.empty()) return ;
    std::map<SlotId, LaunchPlan> launchPlans;
    launchPlans.swap(_slotsPlans);
    if (!_config.launchPlanReady()) {
        CARBON_LOG(WARN, "not config buffer launch plan, use 1st commited role");
        updateConfigLaunchPlan(launchPlans.begin()->second);
    }
    SlotInfoMap slotInfos;
    getAllSlots(&slotInfos);
    for (const auto& kv : launchPlans) {
        const auto& id = kv.first;
        auto it = slotInfos.find(id);
        if (it != slotInfos.end()) {
            vector<SlotInfo> slotInfoVec;
            slotInfoVec.push_back(it->second);
            ProcessContext context;
            fillProcessContext(kv.second, kv.second, &context);
            slotsLaunchPlans->push_back(std::make_pair(slotInfoVec, context));
            setMatcherSlotPlan(id, kv.second);
        } else {
            CARBON_LOG(WARN, "launch process on a non-exist slot [%s]", SlotUtil::toString(id).c_str());
        }
    }

    if (_config.autoUpdateLaunchPlan && _slotPlanMatcher.isAllMatch(_slots.size())) {
        updateConfigLaunchPlan(launchPlans.begin()->second);
    }
}

void BufferedHippoAdapter::launchSlots(
        const std::map<SlotId, LaunchPlan> &launchPlans,
        const std::map<SlotId, LaunchPlan> &finalLaunchPlans)
{
    if (launchPlans.empty()) return ;
    ScopedWriteLock lock(_rwLock);
    for (const auto& kv : launchPlans) {
        _slotsPlans[kv.first] = kv.second;
    }
}

void BufferedHippoAdapter::setMatcherSlotPlan(const SlotId& id, const LaunchPlan& plan) {
    LaunchPlan tmpPlan = plan;
    fixLaunchPlan(tmpPlan);
    _slotPlanMatcher.setSlotPlan(id, tmpPlan);
}

void BufferedHippoAdapter::getAllTags(std::set<tag_t> *tags) const {
    ScopedReadLock lock(_rwLock);
    for (auto e : _tags) {
        tags->insert(e.first);
    }
}

bool BufferedHippoAdapter::isWorking() const {
    return HippoAdapter::isWorking();
}

void BufferedHippoAdapter::releaseTag(const tag_t &tag) {
    ScopedWriteLock lock(_rwLock);
    auto it = _tags.find(tag);
    if (it != _tags.end()) {
        CARBON_LOG(INFO, "release vtag [%s] %lu slots", tag.c_str(), it->second.slots.size());
        assert(it->second.slots.size() == 0);
        _tags.erase(it);
        persistTagSlots();
    } else {
        CARBON_LOG(WARN, "release a non-exist vtag [%s]", tag.c_str());
    }
}

int64_t BufferedHippoAdapter::getAppChecksum() const {
    return HippoAdapter::getAppChecksum();
}

void BufferedHippoAdapter::setConfig(BufferSlotConfig buffConfig, ErrorInfo* errorInfo, bool lock) {
    fixLaunchPlan(buffConfig.launchPlan);
    doSetConfig(buffConfig, errorInfo, lock);
}

void BufferedHippoAdapter::doSetConfig(const BufferSlotConfig& buffConfig, ErrorInfo* errorInfo, bool lock) {
    if (!persistConfig(buffConfig)) {
        CARBON_LOG(ERROR, "persist buffer config failed");
        errorInfo->errorCode = EC_PERSIST_FAILED;
        errorInfo->errorMsg = "persist buffer config failed";
        return ;
    }
    if (lock) {
        ScopedWriteLock lock(_rwLock);
        _config = buffConfig;
    } else {
        _config = buffConfig;
    }
}

BufferSlotConfig BufferedHippoAdapter::getConfig() {
    ScopedReadLock lock(_rwLock);
    return _config;
}

void BufferedHippoAdapter::fixLaunchPlan(LaunchPlan& plan) {
    vector<ProcessInfo> &processInfos = plan.processInfos;
    for (size_t i = 0; i < processInfos.size(); i++) {
        vector<hippo::PairType> &envs = processInfos[i].envs;
        for (auto it = envs.begin(); it != envs.end(); ) {
            const std::string& key = it->first;
            if (key == WORKER_IDENTIFIER_FOR_CARBON) {
                it = envs.erase(it);
                continue;
            } 
            ++it;
        }
    }
    SlotUtil::rewriteProcInstanceId(&plan);
}

void BufferedHippoAdapter::updateConfigLaunchPlan(const LaunchPlan& plan_) {
    LaunchPlan plan = plan_;
    fixLaunchPlan(plan);
    if (_config.launchPlan == plan) return ;
    CARBON_LOG(INFO, "update config launch plan: %s", ToJsonString(plan).c_str());
    BufferSlotConfig config = _config;
    config.launchPlan = plan;
    ErrorInfo errInfo;
    doSetConfig(config, &errInfo, false);
}

void BufferedHippoAdapter::updateConfigResourcePlan(const ResourcePlan& plan) {
    BufferSlotConfig config = _config;
    if (config.resourcePlan == plan) return ;
    CARBON_LOG(INFO, "update config resource plan: %s", ToJsonString(plan).c_str());
    config.resourcePlan = plan;
    ErrorInfo errInfo;
    doSetConfig(config, &errInfo, false);
}

BufferSlotStatus BufferedHippoAdapter::getStatus() const {
    ScopedReadLock lock(_rwLock);
    BufferSlotStatus status;
    status.bufferSlotCount = _buffer.slotSize();
    status.inUseSlotCount = (int32_t) _slots.size();
    _buffer.getSlotsStatus(&status);
    status.slotLaunchPlanSigCounts = _slotPlanMatcher.getSigStatus();
    return status;
}

void BufferedHippoAdapter::preUpdate() {
    ScopedWriteLock lock(_rwLock);
    updateSlots();
}

void BufferedHippoAdapter::postUpdate() {
    ScopedWriteLock lock(_rwLock);
    SlotsLaunchPlanVect slotsLaunchPlans;
    commitSlotsPlans(&slotsLaunchPlans);
    commitBufferSlotsPlans(&slotsLaunchPlans);
    PTR_TEST_CALL(_masterDriver, setSlotProcess(slotsLaunchPlans));

    REPORT_METRIC("", METRIC_BUFFER_SLOT_COUNT, _buffer.slotSize());
    REPORT_METRIC("", METRIC_BUFFER_HEALTH_RATIO, _buffer.getHealthRatio());
}

void BufferedHippoAdapter::updateSlots() {
    SlotInfoMap slotInfos;
    getAllSlots(&slotInfos);
    CARBON_LOG(DEBUG, "get %lu slots from hippo", slotInfos.size());
    for (auto it = slotInfos.begin(); it != slotInfos.end(); ++it) {
        const SlotId& id = it->first;
        auto tit = _tagSlotIds.find(id);
        if (tit != _tagSlotIds.end()) { // the offline slot will be released by Role scheduler
            // fix tag name
            it->second.role = tit->second; 
            _slots[id] = it->second; // update 
            continue;
        } // otherwise the slot is in the buffer
        if (isSlotRecyclable(it->second)) { 
            _buffer.put(it->second);
        } else {
            CARBON_LOG(INFO, "release non recyclable slot [%s]", SlotUtil::toString(id).c_str());
            _buffer.erase(id); // the offlined slot may or may not exist in the buffer 
            ReleasePreference pref;
            pref.type = ReleasePreference::RELEASE_PREF_PROHIBIT;
            _masterDriver->releaseSlot(id, pref);
        }
    }
    _buffer.releaseRedundantSlots(_masterDriver, _config.config.max);
}

void BufferedHippoAdapter::doAlloc(const std::map<tag_t, ResourceRequest> &resourcePlans) {
    for (auto it = resourcePlans.begin(); it != resourcePlans.end(); ++it) {
        const tag_t& tagname = it->first;
        VirtualTag& tag = _tags[tagname];
        if (!tag.countMatch()) {
            SlotInfoMap slotInfos;
            _buffer.alloc(&slotInfos, &tag, tag.count - (int32_t)tag.slots.size());
            if (slotInfos.size() > 0) {
                CARBON_LOG(INFO, "alloc %lu slots from buffer to tag [%s]", slotInfos.size(), tagname.c_str());
                REPORT_METRIC("", METRIC_BUFFER_ALLOC_TIMES, 1);
            }
            for (auto sit = slotInfos.begin(); sit != slotInfos.end(); ++sit) {
                SlotInfo& slot = sit->second;
                tag.addSlot(slot);
                _tagSlotIds[slot.slotId] = tag.name;
            }
            if (tag.countMatch()) {
                int64_t elapsedMs = (autil::TimeUtility::currentTime() - tag.reqSlotAt) / 1000;
                REPORT_METRIC("", METRIC_BUFFER_FEED_SLOT_TIME, elapsedMs);
            }
            _slots.insert(slotInfos.begin(), slotInfos.end());
        }
    }
}

void BufferedHippoAdapter::releaseSlots(const std::map<SlotId, ReleasePreference> &releasedSlots) {
    for (auto it = releasedSlots.begin(); it != releasedSlots.end(); ++it) {
        auto sit = _slots.find(it->first);
        if (sit != _slots.end()) {
            releaseSlot(sit->second, it->second);
        } else {
            CARBON_LOG(WARN, "release a non-existing slot [%s] pref %d", SlotUtil::toString(it->first).c_str(), it->second.type);
            _masterDriver->releaseSlot(it->first, it->second);
        }
    }
    _buffer.releaseRedundantSlots(_masterDriver, _config.config.max);
}

void BufferedHippoAdapter::releaseSlot(const SlotInfo& slot, ReleasePreference pref) {
    const std::string& ss = SlotUtil::toString(slot.slotId);
    const tag_t& tagname = slot.role;
    auto it = _tags.find(tagname);
    if (it != _tags.end()) {
        CARBON_LOG(INFO, "remove slot [%s] from tag [%s]", ss.c_str(), tagname.c_str());
        it->second.removeSlot(slot.slotId);
    } else { // impossible
        CARBON_LOG(WARN, "release a non-associated tag slot [%s]", ss.c_str());
    }
    if (pref.type != ReleasePreference::RELEASE_PREF_PROHIBIT && isSlotRecyclable(slot)) {
        _buffer.recycle(slot, pref);
    } else {
        CARBON_LOG(INFO, "release non recyclable slot [%s] [%d] to hippo", ss.c_str(), pref.type);
        _masterDriver->releaseSlot(slot.slotId, pref);
    }
    _slotPlanMatcher.removeSlot(slot.slotId);
    _tagSlotIds.erase(slot.slotId);
    _slots.erase(slot.slotId); // don't use `slot' after this 
}

bool BufferedHippoAdapter::isSlotRecyclable(const SlotInfo& slot) {
    return !slot.reclaiming && slot.slaveStatus.status != SlaveStatus::DEAD;
}

void BufferedHippoAdapter::setupRoleRequest(RoleRequest* roleRequest) {
    int32_t count = 0;
    for (auto it = _tags.begin(); it != _tags.end(); ++it) {
        count += it->second.count;
    }    
    count += std::min(_config.config.max, std::max(_buffer.slotSize(), _config.config.min));
    ResourceRequest resourceRequest;
    resourceRequest.count = count;
    resourceRequest.resourcePlan = _config.resourcePlan;
    fillRoleRequest(resourceRequest, roleRequest);
}

void BufferedHippoAdapter::getAllSlots(SlotInfoMap* slotInfos) {
    SlotInfoVect slotInfoVect;
    PTR_TEST_CALL(_masterDriver, getSlotsByRole(TAG_POOL_NAME, &slotInfoVect));
    extractSlotInfo(slotInfoVect, slotInfos);
}

void BufferedHippoAdapter::updateTags(const std::map<tag_t, ResourceRequest> &resourcePlans) {
    for (auto it = resourcePlans.begin(); it != resourcePlans.end(); ++it) {
        auto eit = _tags.find(it->first);
        if (eit != _tags.end()) {
            VirtualTag& tag = eit->second;
            if (tag.count != it->second.count) {
                CARBON_LOG(INFO, "update vtag [%s] count %d -> %d", tag.name.c_str(), tag.count, it->second.count);
            }
            tag.setCount(it->second.count);
        } else {
            _tags[it->first] = VirtualTag(it->second.count, it->first);
            CARBON_LOG(INFO, "add a new vtag [%s] %d", it->first.c_str(), it->second.count);
        }
    }
}

// see xxxx://invalid/drogo/fiber/issues/52
// to remove these invalid slots from virtual tags
void BufferedHippoAdapter::sweepTagSlots(const std::vector<tag_t>& tags) {
    std::map<tag_t, std::vector<SlotId>> slotIds;
    {
        ScopedReadLock lock(_rwLock);
        for (const auto& tagName : tags) {
            const auto& tagIt = _tags.find(tagName);
            if (tagIt == _tags.end()) continue;
            const auto& tag = tagIt->second;
            for (const auto& slotId : tag.slots) {
                const auto& it = _slots.find(slotId);
                if (it == _slots.end()) {
                    slotIds[tagName].push_back(slotId);
                }
            }
        }
    }
    if (!slotIds.empty()) {
        ScopedWriteLock lock(_rwLock);
        for (const auto& kv : slotIds) {
            auto tagIt = _tags.find(kv.first);
            if (tagIt == _tags.end()) continue;
            auto& tag = tagIt->second;
            for (const auto& slotId : kv.second) {
                CARBON_LOG(INFO, "sweep invalid slot in tag [%s] [%s]", tag.name.c_str(), 
                        SlotUtil::toString(slotId).c_str());
                tag.removeSlot(slotId);
                _tagSlotIds.erase(slotId);
            }
        }
    }
    if (!slotIds.empty()) {
        ScopedReadLock lock(_rwLock);
        persistTagSlots();
    }
}

bool BufferedHippoAdapter::persistConfig(const BufferSlotConfig& config) {
    std::string content;
    try {
        content = ToJsonString(config);
    } catch (const autil::legacy::ExceptionBase &e) {
        CARBON_LOG(ERROR, "jsonize buffer config failed [%s]", e.what());
        return false;
    }
    return _configSerializer->write(content);
}

bool BufferedHippoAdapter::persistTagSlots() {
    std::string content;
    try {
        content = ToJsonString(_tags);
    } catch (const autil::legacy::ExceptionBase &e) {
        CARBON_LOG(ERROR, "jsonize vtag table failed [%s]", e.what());
        return false;
    }
    return _tagSlotsSerializer->write(content);
}

bool BufferedHippoAdapter::start() {
    // hippo driver load slots in another thread, which means we can't query slots here immediately
    return HippoAdapter::start();
}

void BufferedHippoAdapter::initTagSlotIds() {
    for (const auto& it : _tags) {
        for (const auto& id : it.second.slots) {
            _tagSlotIds[id] = it.second.name;
        }
    }
}

// can only recover slot ids because hippo adapter is not started yet
bool BufferedHippoAdapter::recover() {
    string content;
    WHEN_FS_FILE_EXIST(_tagSlotsSerializer) {
        if (!_tagSlotsSerializer->read(content)) {
            CARBON_LOG(ERROR, "read tag slots data failed");
            return false;
        }
        try {
            FromJsonString(_tags, content);
        } catch (const autil::legacy::ExceptionBase &e) {
            CARBON_LOG(ERROR, "recover tag slots from json failed. error:[%s].", e.what());
            return false;
        }
        initTagSlotIds();
    } END_WHEN();
    content.clear();
    WHEN_FS_FILE_EXIST(_configSerializer) {
        if (!_configSerializer->read(content)) {
            CARBON_LOG(ERROR, "read buffer config data failed");
            return false;
        }
        try {
            FromJsonString(_config, content);
        } catch (const autil::legacy::ExceptionBase &e) {
            CARBON_LOG(ERROR, "recover buffer config from json failed. error:[%s].", e.what());
            return false;
        }
    } END_WHEN();
    CARBON_LOG(INFO, "buffer hippo adapter recover success, [%lu] vtags", _tags.size());
    return true;
}

END_CARBON_NAMESPACE(master);
