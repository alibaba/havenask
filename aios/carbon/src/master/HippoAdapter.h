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
#ifndef CARBON_HIPPOADAPTER_H
#define CARBON_HIPPOADAPTER_H

#include "carbon/CommonDefine.h"
#include "carbon/RolePlan.h"
#include "common/common.h"
#include "carbon/Log.h"
#include "hippo/MasterDriver.h"
#include "autil/Lock.h"

BEGIN_CARBON_NAMESPACE(master);

struct ResourceRequest {
    ResourcePlan resourcePlan;
    int32_t count;
    std::string requirementId;
    LaunchPlan launchPlan;
};

class HippoAdapter
{
public:
    HippoAdapter();
    virtual ~HippoAdapter();
private:
    HippoAdapter(const HippoAdapter &);
    HippoAdapter& operator=(const HippoAdapter &);
public:
    virtual bool init(const std::string &hippoZkRoot,
              worker_framework::LeaderElector *leaderElector,
              const std::string &applicationId);

    virtual bool start();
    
    virtual bool stop();

    virtual bool recover() { return true; }

    virtual void preUpdate();
    virtual void postUpdate() {}

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

protected:
    std::map<tag_t, SlotInfoVect> getSlots() const;

    void fillRoleRequest(
            const ResourceRequest &resourceRequest,
            hippo::RoleRequest *roleRequest);

    void fillProcessContext(
            const LaunchPlan &curLaunchPlan,
            const LaunchPlan &finalLaunchPlan,
            ProcessContext *context);

    void extractSlotInfo(
            const SlotInfoVect &slotInfoVect,
            SlotInfoMap *slotInfos) const;

    void getSlotInfos(std::map<SlotId, SlotInfo> *slotInfos);
    
protected:
    hippo::MasterDriver *_masterDriver;
    mutable autil::ThreadMutex _lock;
    std::map<SlotId, SlotInfo> _allSlots;
private:
    CARBON_LOG_DECLARE();
};

CARBON_TYPEDEF_PTR(HippoAdapter);

END_CARBON_NAMESPACE(master);

#endif //CARBON_HIPPOADAPTER_H
