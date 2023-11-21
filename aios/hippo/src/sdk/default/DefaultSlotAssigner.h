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
#ifndef HIPPO_DEFAULTSLOTASSIGNER_H
#define HIPPO_DEFAULTSLOTASSIGNER_H

#include "util/Log.h"
#include "common/common.h"
#include "hippo/proto/ApplicationMaster.pb.h"
#include "hippo/DriverCommon.h"
#include "hippo/LeaderSerializer.h"

BEGIN_HIPPO_NAMESPACE(sdk);

class DefaultSlotAssigner
{
public:
    DefaultSlotAssigner();
    ~DefaultSlotAssigner();
private:
    DefaultSlotAssigner(const DefaultSlotAssigner &);
    DefaultSlotAssigner& operator=(const DefaultSlotAssigner &);
public:
    bool init(const std::string &applicationId,
              LeaderSerializer* assignedResSerializer,
              LeaderSerializer* ipListReader);

    bool assign(const hippo::proto::AllocateRequest &request,
                hippo::proto::AllocateResponse *response);
    void setLaunchedMetas(const std::map<hippo::SlotId, LaunchMeta> &launchedMetas);

private:
    bool updateMachineResource();
    bool doAssign(const hippo::proto::AllocateRequest &request,
                  hippo::proto::AllocateResponse *response);
    void releaseSlots(const hippo::proto::AllocateRequest &request);
    void clearExclusiveTag(const hippo::SlotId &slotId);
    void clearIpIdx(const std::string& ip, int32_t id);
    void clearIpExclusiveTag(const std::string& ip, const std::string &exclusiveTag);
    void assignSlots(const hippo::proto::AllocateRequest &request,
                     hippo::proto::AllocateResponse *response);
    void assignRoleSlots(const hippo::proto::ResourceRequest &resourceRequest,
                         hippo::proto::ResourceResponse *resourceResponse);
    bool assignNewSlot(const hippo::proto::ResourceRequest &resourceRequest,
                       hippo::proto::ResourceResponse *resourceResponse);
    bool getFirstSpecifiedResource(const hippo::proto::ResourceRequest &resourceRequest,
                                   hippo::proto::Resource::Type resourceType,
                                   std::string &resourceTag);
    bool getFreeNode(const std::vector<std::string> &ipVec,
                     const std::string &exclusiveTag,
                     std::string &ip);
    int32_t getSlotIdx(const std::string& ip);
    void fillAssignedSlot(const hippo::proto::ResourceRequest &resourceRequest,
                          const hippo::SlotId &slotIdIn,
                          hippo::proto::AssignedSlot *slot);
    proto::ProcessStatus::Status getProcessStatus(const hippo::SlotId &slotId, int64_t launchSignature);
    void backUpAssignedResource();
    void deserializeIpInfo(const std::string& ipInfoStr,
                           std::map<std::string, std::vector<std::string> > &ipInfo);
    void serializeAssignedResource(std::string& content) const;
    void deserializeAssignedResource(const std::string& assignedResource);
    void getExclusiveTag(const hippo::SlotId &slotId, std::string &exclusiveTag) const;
    void slotIdToString(const hippo::SlotId &slotId, const std::string &exclusiveTag, std::string &slotIdStr) const;
    void slotIdFromString(std::string &slotIdStr, hippo::SlotId &slotId, std::string &exclusiveTag);
    void setIpInfo(const std::map<std::string, std::vector<std::string> > &ipInfos);
    void recoverAssignedResource(const std::map<std::string, std::vector<std::string> > &role2Ips);

private:
    const static std::string MULTI_SLOTS_IN_ONE_NODE;
    const static std::string DEFAULT_RESOURCE;
    const static int32_t MIN_SLOT_IDX = 0;
    const static int32_t MAX_SLOT_IDX = 1000000;

private:
    bool _multiSlotsOneNode;
    std::string _applicationId;
    LeaderSerializer* _assignedResSerializer;
    LeaderSerializer* _ipListReader;
    std::string _ipInfoStr;
    bool _isReboot;

    std::map<std::string, std::vector<std::string> > _ipInfos; // resource tag to ips
    std::map<std::string, std::set<std::string> > _ipSet;
    std::map<std::string, std::set<hippo::SlotId> > _assignedSlots; // role to slots
    std::map<hippo::SlotId, std::string> _assignedSlot2Role; // slot to role
    std::map<std::string, std::set<int32_t> > _ip2SlotIdxs; // ip to slot idx
    std::map<std::string, std::set<std::string> > _ip2ExclusiveTags;
    std::map<hippo::SlotId, std::string> _assignedSlot2ExclusiveTags;
    std::map<hippo::SlotId, LaunchMeta> _launchedMetas;

    HIPPO_LOG_DECLARE();
};

HIPPO_INTERNAL_TYPEDEF_PTR(DefaultSlotAssigner);

END_HIPPO_NAMESPACE(sdk);

#endif  // HIPPO_DEFAULTSLOTASSIGNER_H
