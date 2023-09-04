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
#ifndef CARBON_GROUPMANAGER_H
#define CARBON_GROUPMANAGER_H

#include "common/common.h"
#include "common/MultiThreadRunner.h"
#include "carbon/Log.h"
#include "master/SerializerCreator.h"
#include "master/Group.h"
#include "master/GroupPlanManager.h"
#include "master/RoleCreator.h"
#include "carbon/Status.h"
#include "autil/Lock.h"
#include "autil/LoopThread.h"

BEGIN_CARBON_NAMESPACE(master);

class GroupManager
{
public:
    GroupManager(const HippoAdapterPtr &hippoAdapterPtr,
                 const GroupPlanManagerPtr &groupPlanManagerPtr,
                 const SerializerCreatorPtr &serializerCreatorPtr,
                 ServiceAdapterExtCreatorPtr creator = ServiceAdapterExtCreatorPtr());
    virtual ~GroupManager();
private:
    GroupManager(const GroupManager &);
    GroupManager& operator=(const GroupManager &);

public:
    bool start();

    void stop();

    bool recover();

    MOCKABLE void getGroupStatus(const std::vector<groupid_t> &groupIds,
                        std::vector<GroupStatus> *groupStatusVect) const;

public:
    bool adjustRunningArgs(const std::string &field,
                           const std::string &value);
    bool releaseSlots(const std::vector<ReleaseSlotInfo> &releaseSlotInfos,
                      ErrorInfo *errorInfo);
    bool genReleaseSlotInfos(const std::vector<SlotId> &slotIds,
                             const uint64_t leaseMs,
                             std::vector<ReleaseSlotInfo> *releaseSlotInfos);

private:
    void schedule();

    GroupPtr createGroup(const groupid_t &groupId);

    void updateGroupPlans(const VersionedGroupPlanMap &groupPlanMap);

    void stopDeletedGroups(const VersionedGroupPlanMap &groupPlanMap);

    void executeOpsCmd();

    void scheduleGroups();

    void releaseTags();

    void genToReleaseTags(std::set<tag_t> *toReleaseTags) const;

    void getResourceTags(std::set<tag_t> *tags) const;

    void getAllGroupStatus(
            std::map<groupid_t, GroupStatus> *groupStatusMap) const;

    void diffAndRecordGroupStatus(
            const std::map<groupid_t, GroupStatus> &groupStatusCache);

    void recordStatus(const GroupStatus &newGroupStatus,
                      const GroupStatus &oriGroupStatus) const;

    bool checkSlotInfoExist(
            const std::vector<ReleaseSlotInfo> &releaseSlotInfos,
            std::vector<ReleaseSlotInfo> *notFoundSlot) const;


    bool __forTestAdjustHippoAdapterStatus(const std::string &value);

    bool __forTestAdjustScheduleInterval(const std::string &value);

private:
    std::map<groupid_t, GroupPtr> _groups;
    GroupPlanManagerPtr _groupPlanManagerPtr;
    SerializerCreatorPtr _serializerCreatorPtr;
    HippoAdapterPtr _hippoAdapterPtr;
    HealthCheckerManagerPtr _healthCheckerManagerPtr;
    ServiceSwitchManagerPtr _serviceSwitchManagerPtr;
    RoleCreatorPtr _roleCreatorPtr;
    std::map<groupid_t, GroupStatus> _groupStatusCache;
    mutable autil::ReadWriteLock _groupStatusLock;

    autil::ThreadMutex _releaseSlotLock;
    std::map<int32_t, ReleaseSlotInfo> _toReleaseSlots;   //slotId->releaseSlotInfo

    common::MultiThreadRunner _runner;
    autil::LoopThreadPtr _scheduleLoopThreadPtr;
private:
    CARBON_LOG_DECLARE();
};

CARBON_TYPEDEF_PTR(GroupManager);

END_CARBON_NAMESPACE(master);

#endif //CARBON_GROUPMANAGER_H
