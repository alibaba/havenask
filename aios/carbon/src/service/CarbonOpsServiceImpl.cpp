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
#include "service/CarbonOpsServiceImpl.h"
#include "carbon/ErrorDefine.h"
#include "carbon/Ops.h"
#include "autil/ClosureGuard.h"
#include "master/BufferedHippoAdapter.h"
#include "hippo/ProtoWrapper.h"
#include "autil/legacy/jsonizable.h"

using namespace std;
USE_CARBON_NAMESPACE(master);
USE_CARBON_NAMESPACE(common);

BEGIN_CARBON_NAMESPACE(service);

CARBON_LOG_SETUP(service, CarbonOpsServiceImpl);

CarbonOpsServiceImpl::CarbonOpsServiceImpl(
        const CarbonDriverPtr &carbonDriverPtr)
{
    _carbonDriverPtr = carbonDriverPtr;
}

CarbonOpsServiceImpl::~CarbonOpsServiceImpl() {
}

#define CHECK_CARBON_MASTER_READY() do {                                \
        GroupPlanManagerPtr groupPlanManagerPtr = _carbonDriverPtr->getGroupPlanManager(); \
        GroupManagerPtr groupManagerPtr = _carbonDriverPtr->getGroupManager(); \
        if (groupPlanManagerPtr == NULL || groupManagerPtr == NULL) {   \
            ErrorInfo errorInfo;                                        \
            errorInfo.errorCode = EC_CARBON_MASTER_NOT_READY;           \
            errorInfo.errorMsg = "carbon master is not ready";          \
            CARBON_LOG(ERROR, "%s", errorInfo.errorMsg.c_str());        \
            *response->mutable_errorinfo() = autil::legacy::ToJsonString(errorInfo, true); \
            return;                                                     \
        }                                                               \
    } while (0)

#define CALL(func, ...) {                                               \
        ErrorInfo errorInfo;                                            \
        _carbonDriverPtr->getGroupPlanManager()->func(__VA_ARGS__, &errorInfo); \
        *response->mutable_errorinfo() = autil::legacy::ToJsonString(errorInfo, true); \
    }

void CarbonOpsServiceImpl::releaseSlots(
        ::google::protobuf::RpcController* controller,
        const ::carbon::proto::ReleaseSlotsRequest* request,
        ::carbon::proto::ReleaseSlotsResponse* response,
        ::google::protobuf::Closure* done)
{
    CARBON_LOG(INFO, "release slots, request:%s", request->ShortDebugString().c_str());
    autil::ClosureGuard closure(done);
    CHECK_CARBON_MASTER_READY();

#define CHECK_FIELD_EXIST(objName, parentNode, node)                    \
    do {                                                                \
        if (!parentNode.has_##node()) {                                 \
            ErrorInfo errorInfo;                                        \
            errorInfo.errorCode = EC_FIELD_NOT_EXIST;                   \
            errorInfo.errorMsg = "field " #node " not exist.";          \
            CARBON_LOG(ERROR, "%s", errorInfo.errorMsg.c_str());        \
            *response->mutable_errorinfo() = autil::legacy::ToJsonString(errorInfo, true); \
            return;                                                     \
        }                                                               \
    } while (0)
    
    vector<ReleaseSlotInfo> releaseSlotInfos;
    for (int i = 0; i < request->releaseslots_size(); i++) {
        ReleaseSlotInfo releaseSlotInfo;
        const auto &releaseslot = request->releaseslots(i);
        CHECK_FIELD_EXIST(carbonUnitId, releaseslot, carbonunitid);
        const auto &carbonunitid = releaseslot.carbonunitid();
        CHECK_FIELD_EXIST(groupId, carbonunitid, groupid);
        releaseSlotInfo.carbonUnitId.groupId = carbonunitid.groupid();
        CHECK_FIELD_EXIST(roleId, carbonunitid, roleid);
        releaseSlotInfo.carbonUnitId.roleId = carbonunitid.roleid();
        CHECK_FIELD_EXIST(hippoSlotId, carbonunitid, hipposlotid);
        releaseSlotInfo.carbonUnitId.hippoSlotId = carbonunitid.hipposlotid();
        releaseSlotInfo.leaseMs = releaseslot.leasems();

        releaseSlotInfos.push_back(releaseSlotInfo);
    }
#undef CHECK_FIELD_EXIST
    GroupManagerPtr groupManagerPtr = _carbonDriverPtr->getGroupManager();
    RouterPtr routerPtr = _carbonDriverPtr->getRouter();
    vector<ReclaimWorkerNode> toReclaimWorkerNodeList;
    if (request->slotids_size() > 0) {
        vector<ReleaseSlotInfo> tmpReleaseSlotInfos;
        vector<SlotId> slotIds;
        SlotId slotId;
        for (int i = 0; i < request->slotids_size(); i++) {
            hippo::ProtoWrapper::convert(request->slotids(i), &slotId);
            slotIds.push_back(slotId);
        }
        groupManagerPtr->genReleaseSlotInfos(slotIds,
                request->leasems(), &tmpReleaseSlotInfos);
        releaseSlotInfos.insert(releaseSlotInfos.end(),
                                tmpReleaseSlotInfos.begin(), tmpReleaseSlotInfos.end());
        routerPtr->genReclaimWorkerNodeList(slotIds, &toReclaimWorkerNodeList);
    }

    ErrorInfo errorInfo;
    if (!routerPtr->releaseSlots(toReclaimWorkerNodeList, request->leasems(), &errorInfo)){
         CARBON_LOG(WARN, "release slot failed.");
    }

    if (!groupManagerPtr->releaseSlots(releaseSlotInfos, &errorInfo)) {
        CARBON_LOG(WARN, "release slot failed.");
    }
    *response->mutable_errorinfo() = autil::legacy::ToJsonString(errorInfo, true);
}

void CarbonOpsServiceImpl::sweepTagSlots(::google::protobuf::RpcController* controller,
                  const ::carbon::proto::SweepSlotsRequest* request,
                  ::carbon::proto::SweepSlotsResponse* response,
                  ::google::protobuf::Closure* done)
{
    CARBON_LOG(INFO, "sweepTagSlots, request:%s", request->ShortDebugString().c_str());
    autil::ClosureGuard closure(done);
    CHECK_CARBON_MASTER_READY();
    HippoAdapterPtr adapterPtr = _carbonDriverPtr->getBufferHippoAdapter();
    ErrorInfo errorInfo;
    if (adapterPtr) {
        BufferedHippoAdapter* bufferedAdapter = (BufferedHippoAdapter*) adapterPtr.get();    
        vector<tag_t> tags;
        for (int i = 0; i < request->tagids_size(); i++) {
            tags.push_back(request->tagids(i));
        }
        bufferedAdapter->sweepTagSlots(tags);
    } else {
        errorInfo.errorMsg = "only buffer hippo adapter support";
    }
    *response->mutable_errorinfo() = autil::legacy::ToJsonString(errorInfo, true);
}

void CarbonOpsServiceImpl::syncGroupPlans(::google::protobuf::RpcController* controller,
                  const ::carbon::proto::SyncGroupRequest* request,
                  ::carbon::proto::SyncGroupResponse* response,
                  ::google::protobuf::Closure* done)
{
    CARBON_LOG(INFO, "syncGroupPlans called, request: %s", request->ShortDebugString().c_str());
    autil::ClosureGuard closure(done);
    CHECK_CARBON_MASTER_READY();
    RouterPtr router = _carbonDriverPtr->getRouter();
    ErrorInfo errorInfo;
    string *plans = response->mutable_plans();
    router->syncGroups(Router::SyncOptions(request->fixcount(), request->dryrun()), &errorInfo, plans);
    *response->mutable_errorinfo() = autil::legacy::ToJsonString(errorInfo, true);
}

END_CARBON_NAMESPACE(service);

