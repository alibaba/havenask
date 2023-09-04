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
#ifndef HIPPO_SLOTALLOCATORBASE_H
#define HIPPO_SLOTALLOCATORBASE_H

#include "util/Log.h"
#include "common/common.h"
#include "autil/Lock.h"
#include "hippo/proto/Common.pb.h"
#include "hippo/proto/ApplicationMaster.pb.h"
#include "hippo/DriverCommon.h"
#include "hippo/MasterEvent.h"

BEGIN_HIPPO_NAMESPACE(sdk);

typedef std::string RoleName;
typedef std::map<RoleName, RoleRequest> RoleResourceRequestMap;

typedef std::vector<hippo::SlotInfo*> SlotInfos;
typedef std::map<hippo::SlotId, hippo::SlotInfo*> SlotInfoMap;
typedef std::map<RoleName, std::map<hippo::SlotId, hippo::SlotInfo*> > RoleSlotInfoMap;

typedef std::function<
    void (MasterDriverEvent events, const SlotInfos*, std::string)> EventTrigger;

struct ReleaseOptions {
    hippo::ReleasePreference preference;
    int32_t reserveTime;
    ReleaseOptions(const hippo::ReleasePreference &pref = hippo::ReleasePreference(),
                   int32_t time = 0)
        : preference(pref), reserveTime(time)
    {}
};

class SlotAllocatorBase
{
public:
    SlotAllocatorBase(EventTrigger *eventTrigger);
    virtual ~SlotAllocatorBase();
private:
    SlotAllocatorBase(const SlotAllocatorBase &);
    SlotAllocatorBase& operator=(const SlotAllocatorBase &);
public:
    virtual void updateResourceRequest(
            const std::string &role,
            const hippo::RoleRequest &request);

    void releaseSlot(const hippo::SlotId &slotId,
                     const hippo::ReleasePreference &pref,
                     int32_t slotReserveTime = 0);

    void releaseRoleSlots(const std::string &role,
                          const hippo::ReleasePreference &pref,
                          int32_t roleSlotReserveTime = 0);

    void getSlots(std::vector<hippo::SlotInfo> *slots) const;

    void getSlotsByRole(const std::string &role,
                        std::vector<hippo::SlotInfo> *slots) const;

    void getReclaimingSlots(std::vector<hippo::SlotInfo> *slots) const;

    bool getSlotInfo(const hippo::SlotId &slotId, SlotInfo *slotInfo) const;

    bool allocate();

    virtual void setApplicationId(const std::string &appId);

    void getRoleNames(std::set<std::string> *roleNames) const;

    hippo::ErrorInfo getLastErrorInfo() const {
        return _lastErrorInfo;
    }

public:
    //for test
    RoleResourceRequestMap getResourceRequests() const {
        return _resourceRequests;
    }
    std::map<hippo::SlotId, ReleaseOptions> getNeedReleaseSlots() const {
        return _needReleaseSlots;
    }
    std::map<RoleName, ReleaseOptions> getNeedReleaseRoles() const {
        return _needReleaseRoles;
    }
    void setAllocatedSlots(const std::string &role, const SlotInfos &slotInfos) {
        SlotInfoMap &slotInfoMap = _allocatedSlots[role];
        for (size_t i = 0; i < slotInfos.size(); i++) {
            slotInfos[i]->role = role;
            slotInfoMap[slotInfos[i]->slotId] = slotInfos[i];
        }
    }

protected:
    virtual bool doAllocate(const proto::AllocateRequest &request,
                            proto::AllocateResponse *response) = 0;

private:
    void createAllocateRequest(proto::AllocateRequest *request) const;

    void processResponse(const proto::AllocateResponse &response);

    void fillResourceRequest(proto::AllocateRequest *request) const;

    void fillReleaseSlots(proto::AllocateRequest *request) const;

    bool needRelease(const RoleName &role, const hippo::SlotId &slotId,
                     ReleaseOptions *releaseOpts = NULL) const;

    void generateAllocatedSlots(const proto::AllocateResponse &response,
                                RoleSlotInfoMap *newAllocatedSlots) const;

    void triggerSlotEvents(const RoleSlotInfoMap &lastAllocatedSlots,
                           const RoleSlotInfoMap &newAllocatedSlots) const;

    void trigger(hippo::MasterDriverEvent events, const SlotInfos &slotInfos,
                 std::string message) const;

    void diffSlotInfos(const SlotInfoMap &oldSlotInfos,
                       const SlotInfoMap &newSlotInfos,
                       SlotInfos *newAllocSlots,
                       SlotInfos *reclaimingSlots,
                       SlotInfos *reclaimedSlots,
                       SlotInfos *resourceChangeSlots,
                       SlotInfos *statusChangeSlots) const;

    void diffSlotInfo(hippo::SlotInfo *oldSlotInfo,
                      hippo::SlotInfo *newSlotInfo,
                      SlotInfos *reclaimingSlots,
                      SlotInfos *resourceChangeSlots,
                      SlotInfos *statusChangeSlots) const;

    bool slotStatusChange(const hippo::SlotInfo *oldSlotInfo,
                          const hippo::SlotInfo *newSlotInfo) const;

    bool processStatusChange(const hippo::SlotInfo *oldSlotInfo,
                             const hippo::SlotInfo *newSlotInfo) const;

    bool slotResourceChange(const hippo::SlotInfo *oldSlotInfo,
                            const hippo::SlotInfo *newSlotInfo) const;

    void clearReleasedSlots(const RoleSlotInfoMap &allocatedSlots);

    void clearRoleSlotInfoMap(RoleSlotInfoMap *roleSlotInfosMap) const;

protected:
    EventTrigger *_eventTrigger;
    mutable autil::ThreadMutex _mutex;

    std::string _applicationId;

    // each role's resource request filled by user
    RoleResourceRequestMap _resourceRequests;

    // need to be released slots filled by user
    std::map<hippo::SlotId, ReleaseOptions> _needReleaseSlots;

    // need to be released roles filled by user
    std::map<RoleName, ReleaseOptions> _needReleaseRoles;

    // allocated slots from hippo master
    RoleSlotInfoMap _allocatedSlots;

    hippo::ErrorInfo _lastErrorInfo;
    HIPPO_LOG_DECLARE();
};

HIPPO_INTERNAL_TYPEDEF_PTR(SlotAllocatorBase);

END_HIPPO_NAMESPACE(sdk);

#endif  // HIPPO_SLOTALLOCATORBASE_H
