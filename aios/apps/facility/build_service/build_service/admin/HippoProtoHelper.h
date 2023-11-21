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
#pragma once

#include "build_service/proto/ProtoJsonizer.h"
#include "build_service/proto/WorkerNode.h"
#include "hippo/DriverCommon.h"
#include "hippo/HippoUtil.h"

namespace build_service::admin {

class HippoProtoHelper
{
public:
    static void setSlotInfo(const std::vector<hippo::SlotInfo>& slotInfos, proto::WorkerNodeBase* workerNode)
    {
        proto::WorkerNodeBase::PBSlotInfos pbSlotInfos;
        for (const auto& slotInfo : slotInfos) {
            *pbSlotInfos.Add() = convertSlotInfo(slotInfo);
        }
        workerNode->setSlotInfo(pbSlotInfos);
    }

    static std::string getSlotInfoJsonString(const proto::WorkerNodeBase::PBSlotInfos& pbSlotInfos)
    {
        std::vector<std::string> strVec;
        for (const auto& pbSlotInfo : pbSlotInfos) {
            strVec.push_back(proto::ProtoJsonizer::toJsonString(pbSlotInfo));
        }
        return autil::legacy::ToJsonString(strVec);
    }

    template <typename WorkNode>
    static std::string getNodeIdentifier(const WorkNode& node)
    {
        std::string identifier;
        auto slotInfos = node.getPartitionInfo().slotinfos();
        for (size_t i = 0; i < slotInfos.size(); i++) {
            if (!slotInfos[i].reclaim()) {
                assert(identifier.empty());
                auto ip = hippo::HippoUtil::getIp(slotInfos[i]);
                identifier = proto::WorkerNodeBase::getIdentifier(ip, slotInfos[i].id().id());
            }
        }
        return identifier;
    }

private:
    static hippo::proto::AssignedSlot convertSlotInfo(const hippo::SlotInfo& slotInfo)
    {
        hippo::proto::AssignedSlot pbSlotInfo;
        auto pbSlotId = pbSlotInfo.mutable_id();
        pbSlotId->set_slaveaddress(slotInfo.slotId.slaveAddress);
        pbSlotId->set_id(slotInfo.slotId.id);
        pbSlotId->set_declaretime(slotInfo.slotId.declareTime);
        pbSlotInfo.set_reclaim(slotInfo.reclaiming);

        for (const auto& resource : slotInfo.slotResource.resources) {
            auto pbResource = pbSlotInfo.mutable_slotresource()->add_resources();
            pbResource->set_type(hippo::proto::Resource_Type(resource.type));
            pbResource->set_name(resource.name);
            pbResource->set_amount(resource.amount);
        }
        for (const auto& processStatus : slotInfo.processStatus) {
            auto pbProcessStatus = pbSlotInfo.add_processstatus();
            pbProcessStatus->set_status(hippo::proto::ProcessStatus_Status(processStatus.status));
            pbProcessStatus->set_processname(processStatus.processName);
            pbProcessStatus->set_starttime(processStatus.startTime);
            pbProcessStatus->set_restartcount(processStatus.restartCount);
            pbProcessStatus->set_exitcode(processStatus.exitCode);
            pbProcessStatus->set_pid(processStatus.pid);
        }
        return pbSlotInfo;
    }
};

} // namespace build_service::admin
